import sys

import quickfix as qf

from orderpush.fix_application import FixApplication
from orderpush.utils import Configs, generate_id

if __name__ == "__main__":
    configs = Configs(sys.argv[1])
    id_generator = generate_id(configs.snowflake_node_id)

    application = FixApplication(id_generator)
    settings = qf.SessionSettings(configs.fix_config)
    storefactory = qf.FileStoreFactory(settings)
    logfactory = qf.FileLogFactory(settings)
    initiator = qf.SocketInitiator(application, storefactory, settings, logfactory)

    initiator.start()
    application.run()
