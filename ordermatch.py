import argparse
import logging
import os
import sys
from logging.config import dictConfig

import quickfix as qf
import yaml

from ordermatch import Application


def main(fix_config: str, node_id: int):
    try:
        settings = qf.SessionSettings(fix_config)
        application = Application(node_id)
        storefactory = qf.FileStoreFactory(settings)
        logfactory = qf.FileLogFactory(settings)
        acceptor = qf.SocketAcceptor(application, storefactory, settings, logfactory)

        acceptor.start()
        application.run()

    except (KeyboardInterrupt, SystemExit):
        logging.info("Shuttting down")
    except (qf.ConfigError, qf.RuntimeError) as e:
        logging.exception("Quickfix error")
    except Exception as e:
        logging.exception("Something went wrong")
    finally:
        acceptor.stop()
        sys.exit()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("fix_config")
    parser.add_argument("settings")
    args = parser.parse_args()

    with open(args.settings) as f:
        settings = yaml.safe_load(f)

    # Config logging
    logging_config = settings.get("logging")
    logs_dir = os.path.dirname(logging_config.get("handlers").get("file").get("filename"))
    os.makedirs(logs_dir, exist_ok=True)
    dictConfig(logging_config)

    main(args.fix_config, settings.get("snowflake_node_id"))
