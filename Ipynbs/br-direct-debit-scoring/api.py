#!/usr/bin/env python3
# vim: set ai et ff=unix sts=4 sw=4 ts=4 :
# -*- coding: utf-8 -*-

############################################################
# Package: Python Component API
# Version: 0.1
# Author: Kirill Shtrykov <kirill@shtrykov.com>
# Website: https://shtrykov.com
############################################################

import os
import sys
import logging
from aiohttp import web
from json import JSONDecodeError
import dd_scoring_script


class API(object):
    def __init__(self):
        """
        Инициализация API
        """
        debug = os.getenv('DEBUG')
        log_level = 40
        if debug:
            log_level = 10
        logging.basicConfig(level=log_level)
        log = logging.getLogger('aiohttp.access')
        log.setLevel(logging.ERROR)
        logging.info("Python Component API")

    def start_server(self):
        """
        Старт web-сервера
        :return:
        """
        app = web.Application()
        app.router.add_head('/', self.index)
        app.router.add_post('/in', self.api)
        web.run_app(app=app, host='127.0.0.1', port=9999)

    @staticmethod
    def index(_):
        """
        Обработчик index
        :return web.Response:
        """
        return web.Response(text="OK")

    async def api(self, request):
        """
        Обработчик запросов к API
        :param request:
        :return: web.Response()
        """
        try:
            data = await request.json()
        except JSONDecodeError as error:
            return web.Response(text="Error request: %s" % error)
        except:
            return web.Response(text="Error request: %s" % sys.exc_info()[0])
        logging.debug(data)
        return web.Response(text=str(dd_scoring_script.main(data)))


if __name__ == "__main__":
    api = API()
    api.start_server()
