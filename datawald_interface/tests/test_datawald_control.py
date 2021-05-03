import logging, sys, json, unittest, uuid, os
from datetime import datetime, timedelta, date
from decimal import Decimal
from pathlib import Path
from silvaengine_utility import Utility

from dotenv import load_dotenv

load_dotenv()
setting = {
    "region_name": os.getenv("region_name"),
    "aws_access_key_id": os.getenv("aws_access_key_id"),
    "aws_secret_access_key": os.getenv("aws_secret_access_key"),
}

sys.path.insert(0, "/var/www/projects/datawald_interface")
sys.path.insert(1, "/var/www/projects/datawald_model")

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger()

from datawald_interface import Control


class DataWaldControlTest(unittest.TestCase):
    def setUp(self):
        self.control = Control(logger, **setting)
        logger.info("Initiate DataWaldControlTest ...")

    def tearDown(self):
        logger.info("Destory DataWaldControlTest ...")

    @unittest.skip("demonstrating skipping")
    def test_graphql_insertsynctask(self):
        mutation = """
            mutation InsertSyncTask(
                $task: String!,
                $source: String!,
                $target: String!,
                $table: String!,
                $cutDate: DateTime!,
                $offset: Int!,
                $entities: [EntityInputType]!
            ) {
                insertSyncTask(
                    task: $task,
                    source: $source,
                    target: $target,
                    table: $table,
                    cutDate: $cutDate,
                    offset: $offset,
                    entities: $entities
                ) {
                    syncTask {
                        task
                        id
                        source
                        target
                        table
                        cutDate
                        startDate
                        endDate
                        offset
                        syncNote
                        syncStatus
                        entities {
                            source
                            id
                            taskNote
                            taskStatus
                            updatedAt
                        }
                    }
                }
            }
        """

        variables = {
            "task": "syncOrder",
            "source": "MAGE2SQS",
            "target": "NS-MAGE2",
            "table": "transaction",
            "cutDate": "2021-04-19T12:24:04.235901+0000",
            "offset": 0,
            "entities": [
                {
                    "source": "",
                    "id": "xxxxxxx",
                    "taskNote": "xxxxxxx",
                    "taskStatus": "S",
                    "updatedAt": "2021-04-19T12:24:04.235901+0000"
                }
            ]
        }

        payload = {"mutation": mutation, "variables": variables}

        response = self.control.control_graphql(**payload)
        logger.info(response)

    @unittest.skip("demonstrating skipping")
    def test_graphql_updatesynctask(self):
        mutation = """
            mutation UpdateSyncTask(
                $task: String!,
                $id: String!,
                $entities: [EntityInputType]!
            ) {
                updateSyncTask(
                    task: $task,
                    id: $id,
                    entities: $entities
                ) {
                    syncTask {
                        task
                        id
                        source
                        target
                        table
                        cutDate
                        startDate
                        endDate
                        offset
                        syncNote
                        syncStatus
                        entities {
                            source
                            id
                            taskNote
                            taskStatus
                            updatedAt
                        }
                    }
                }
            }
        """

        variables = {
            "task": "syncOrder",
            "id": "12118427224148808171",
            "entities": [
                {
                    "source": "MAGE2SQS",
                    "id": "xxxxxxx",
                    "taskNote": "xxxxxxx",
                    "taskStatus": "S",
                    "updatedAt": "2021-04-19T12:24:04.235901+0000"
                }
            ]
        }

        payload = {"mutation": mutation, "variables": variables}

        response = self.control.control_graphql(**payload)
        logger.info(response)

    @unittest.skip("demonstrating skipping")
    def test_graphql_gettask(self):
        logger.info(sys._getframe().f_code.co_name)

        query = """
        query getTask($table: String!, $source: String!, $id: String!){
            task(table: $table, source: $source, id: $id) {
                source
                id
                taskStatus
                taskNote
                updatedAt
                ready
            }
        }
        """

        variables = {"table": "transaction", "source": "MAGE2SQS", "id": "594efb4a-91d3-11eb-b0eb-d8f2cab5f526"}

        payload = {"query": query, "variables": variables}

        response = self.control.control_graphql(**payload)
        logger.info(response)

    # @unittest.skip("demonstrating skipping")
    def test_graphql_getcutdate(self):
        logger.info(sys._getframe().f_code.co_name)

        query = """
        query getCutDate($source: String!, $task: String!){
            cutDate(source: $source, task: $task) {
                cutDate
                offset
            }
        }
        """

        variables = {"source": "MAGE2SQS", "task": "syncOrder"}

        payload = {"query": query, "variables": variables}

        response = self.control.control_graphql(**payload)
        logger.info(response)


if __name__ == "__main__":
    unittest.main()