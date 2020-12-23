import os
import sys
import logging
import argparse
import datetime
import multiprocessing
import time
import uuid
from copy import deepcopy
from typing import Optional

import yaml

from crontab import crontab_match

# patch import path
script_dir = os.path.abspath(os.path.dirname(__file__))
crawler_dir = os.path.join(script_dir, 'biothings.crawler')
sys.path.append(crawler_dir)

# Scrapy
os.environ['SCRAPY_SETTINGS_MODULE'] = 'crawler.settings'

# patch PATH so local venv is in PATH
bin_path = os.path.join(script_dir, 'venv/bin')
os.environ['PATH'] += os.pathsep + bin_path
# patch PATH so interpreter dir is also in PATH
os.environ['PATH'] += os.pathsep + \
                      os.path.abspath(os.path.dirname(sys.executable))

from scrapy.spiderloader import SpiderLoader
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from elasticsearch import Elasticsearch

from crawler.upload import uploaders


def get_build_timestamp(es: Elasticsearch, alias_name: str) -> Optional[int]:
    try:
        om = es.indices.get_mapping(alias_name, include_type_name=False)
        if len(om.keys()) != 1:
            raise ValueError()
        o_idx_name = list(om.keys())[0]
        orig_build_date = om[o_idx_name]['mappings']['_meta']['build_date']
        orig_build_date = datetime.datetime.fromisoformat(orig_build_date)
        # check timezone
        if orig_build_date.tzinfo is None:
            orig_build_date.astimezone()  # force to local timezone
            # anecdote: if timezones are properly configured, should handle
            # dst properly
        orig_build_date = int(orig_build_date.timestamp())
    except:
        orig_build_date = None
    return orig_build_date


def invoke_crawl(es_host: str, es_index: str, crawler: str):
    # crawler uses env vars for this
    os.environ['ES_HOST'] = es_host
    os.environ['ES_INDEX'] = es_index
    # crawl
    process = CrawlerProcess(get_project_settings())
    process.crawl(crawler)
    process.start()
    process.join()


def alias_switcheroo(es: Elasticsearch, alias_name: str, index_name: str):
    # alias update
    if not es.indices.exists(alias_name):
        es.indices.put_alias(index=index_name, name=alias_name)
    else:
        # if an index not alias exists, just let it crash
        actions = {
            "actions": [
                {"add": {"index": index_name, "alias": alias_name}}
            ]
        }
        rm_idx = [i_name for i_name in es.indices.get_alias(alias_name)]
        removes = [{
            "remove": {"index": index_name, "alias": alias_name}
        } for index_name in rm_idx
        ]
        actions["actions"].extend(removes)
        es.indices.update_aliases(actions)
        # delete old indices
        for rm_i in rm_idx:
            es.indices.delete(rm_i)


def perform_crawl_and_update(
        crawler: str, uploader: str, alias_name: str,
        es_host_c: str, es_host_u: str,
        es_idx_c: Optional[str] = None,
        es_idx_u: Optional[str] = None,
        log_path: str = None
):
    # FIXME: somehow it is still littering standard output
    if log_path is not None:
        logging.basicConfig(filename=log_path)
    es_crawler = Elasticsearch(es_host_c)
    es_uploader = Elasticsearch(es_host_u)
    if es_idx_c is None or es_idx_u is None:
        flag = True
        while flag:
            u = uuid.uuid1()
            tmp_idx_c = f"crawler_{crawler}_{u.hex}"
            tmp_idx_u = f"uploader_{uploader}_{u.hex}"
            flag1 = es_crawler.indices.exists(tmp_idx_c) and es_idx_c is None
            flag2 = es_uploader.indices.exists(tmp_idx_u) and es_idx_u is None
            flag = flag1 or flag2  # both idx names: set or not already exist
        if es_idx_u is None:
            es_idx_u = tmp_idx_u
        if es_idx_c is None:
            es_idx_c = tmp_idx_c
    # crawl
    invoke_crawl(es_host_c, es_idx_c, crawler)
    # force a refresh, might cause performance issues
    # will change this if that happens
    es_crawler.indices.refresh(index=es_idx_c)
    # upload
    uploader = uploaders[uploader](
        src_host=es_host_c,
        src_index=es_idx_c,
        dest_host=es_host_u,
        dest_index=es_idx_u
    )
    uploader.upload()
    # update alias
    alias_switcheroo(es_uploader, alias_name, es_idx_u)
    es_crawler.indices.delete(es_idx_c)


if __name__ == '__main__':
    multiprocessing.set_start_method('spawn')
    logging.basicConfig(format='%(asctime)s %(message)s')
    # load environment
    scrapy_settings = get_project_settings()
    spiders = SpiderLoader.from_settings(scrapy_settings).list()

    # handle arguments
    parser = argparse.ArgumentParser(description="Crawl and update a source")
    subparsers = parser.add_subparsers(dest='action')
    runyaml_parser = subparsers.add_parser('runyaml', help='run from YAML doc')
    runyaml_parser.add_argument('--yaml', required=True)
    run_parser = subparsers.add_parser('runcmd', help='run from command line')
    run_parser.add_argument('--crawler', '-c',
                            type=str, choices=spiders, required=True)
    run_parser.add_argument('--uploader', '-u',
                            type=str, choices=uploaders.keys(), required=True)
    run_parser.add_argument('--es-host-crawler', '-ehc',
                            type=str, default='localhost')
    run_parser.add_argument('--es-index-crawler', '-eic', type=str,
                            help="""Index name for crawler to use. If omitted,
                        a proper random name will be chosen.
                        This index is deleted after the uploader has completed
                        running.""")
    run_parser.add_argument('--es-host-uploader', '-ehu',
                            type=str, default='localhost')
    run_parser.add_argument('--es-index-uploader', '-eiu', type=str,
                            help="""Index name for uploader to use. If omitted,
                        a proper random name will be chosen. This index is kept
                        until next successful run.""")
    run_parser.add_argument('--target-alias', '-a', type=str, required=True,
                            help="""Target alias""")
    args = parser.parse_args()

    tasks = {}
    if args.action == 'runyaml':
        with open(args.yaml) as f:
            # FIXME: fix path problem
            config = yaml.load(f, Loader=yaml.FullLoader)
        for k, v in config.items():
            task = {
                'crawler': v['crawler'],
                'uploader': v['uploader'],
                'es_host_c': v['crawler_host'],
                'es_host_u': v['uploader_host'],
                'es_idx_c': v.get('crawler_index'),
                'es_idx_u': v.get('uploader_index'),
                'alias_name': v['alias_name'],
            }
            if 'crontab' in v:
                task['crontab'] = v['crontab']
            tasks[k] = task

    elif args.action == 'runcmd':
        tasks['cmdline'] = {
                'crawler': args.crawler,
                'uploader': args.uploader,
                'es_host_c': args.es_host_crawler,
                'es_host_u': args.es_host_uploader,
                'es_idx_c': args.es_index_crawler,
                'es_idx_u': args.es_index_uploader,
                'alias_name': args.target_alias,
        }
    else:
        pass

    # process the run once items
    ks = list(tasks.keys())
    for k in ks:
        v = tasks[k]
        if 'crontab' in v:
            pass
        else:
            t = datetime.datetime.now()
            log_path = f"{k}_{t.strftime('%Y%m%dT%H%M%S')}.log"
            kwa = {'log_path': log_path}
            kwa.update(v)
            p = multiprocessing.Process(target=perform_crawl_and_update,
                                        kwargs=kwa)
            logging.info("Executing %s once ...", k)
            p.start()
            # does not wait for it to end
            tasks.pop(k)

    # exit if no recurring tasks
    if len(tasks) == 0:
        sys.exit(0)

    # handle the remaining
    running_tasks = {}
    while True:
        # FIXME: I just noticed that chromedriver doesn't quit after
        #  running the crawlers. Should implement something that kills
        #  all my chrome/chromedriver process when nothing is running.
        t = datetime.datetime.now()
        running_tasks_names = list(running_tasks.keys())
        for task_name in running_tasks_names:
            p = running_tasks[task_name]
            if p.is_alive():
                continue
            else:
                running_tasks.pop(task_name)
                logging.info("%s finished running.", task_name)
        for k, v in tasks.items():
            kwa = deepcopy(v)
            crontab_entry = kwa.pop('crontab')
            if crontab_match(crontab_entry, t):
                if k in running_tasks:
                    logging.warning("Task %s is not yet completed, not run.")
                    continue
                log_path = f"{k}_{t.strftime('%Y%m%dT%H%M%S')}.log"
                kwa['log_path'] = log_path
                p = multiprocessing.Process(target=perform_crawl_and_update,
                                            kwargs=kwa)
                running_tasks[k] = p
                logging.info("executing %s ...", k)
                p.start()

        t = datetime.datetime.now()
        time.sleep(60 - t.second - t.microsecond/1000000)
