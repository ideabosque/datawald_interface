#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function

__author__ = "bibow"

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

from datawald_interface import Interface


class DataWaldInterfaceTest(unittest.TestCase):
    def setUp(self):
        self.interface = Interface(logger, **setting)
        logger.info("Initiate DataWaldInterfaceTest ...")

    def tearDown(self):
        logger.info("Destory DataWaldInterfaceTest ...")

    @unittest.skip("demonstrating skipping")
    def test_graphql_insertorder(self):
        mutation = """
            mutation InsertTransaction($txType: String!, $source: String!, $srcId: String!, $data: String!) {
                insertTransaction(txType: $txType, source: $source, srcId: $srcId, data: $data) {
                    transaction {
                        __typename
                        id
                        source
                        srcId
                        tgtId
                        txType
                        ... on OrderType {
                            data{
                                orderStatus
                                addresses{
                                    billto{
                                        address
                                        city
                                        company
                                        country
                                        email
                                        firstname
                                        lastname
                                        postcode
                                        region
                                        telephone
                                    }
                                    shipto{
                                        address
                                        city
                                        company
                                        country
                                        email
                                        firstname
                                        lastname
                                        postcode
                                        region
                                        telephone
                                    }
                                }
                                items{
                                    price
                                    qty
                                    sku
                                }
                            }
                            history{
                                tgtId
                                updatedAt
                                orderStatus
                                addresses{
                                    billto{
                                        address
                                        city
                                        company
                                        country
                                        email
                                        firstname
                                        lastname
                                        postcode
                                        region
                                        telephone
                                    }
                                    shipto{
                                        address
                                        city
                                        company
                                        country
                                        email
                                        firstname
                                        lastname
                                        postcode
                                        region
                                        telephone
                                    }
                                }
                                items{
                                    price
                                    qty
                                    sku
                                }
                            }
                        }
                        createdAt
                        updatedAt
                        txNote
                        txStatus
                    }
                }
            }
        """

        data = {
            "order_status": "new",
            "addresses": {
                "billto": {
                    "address": "111 E. Main Street",
                    "city": "Holdenville",
                    "company": ".",
                    "country": "US",
                    "email": "rclark@telecomservicebureau.com",
                    "firstname": "Easy Wireless Store",
                    "lastname": "21",
                    "postcode": "74848",
                    "region": "OK",
                    "telephone": ".",
                },
                "shipto": {
                    "address": "111 E. Main Street",
                    "city": "Holdenville",
                    "company": ".",
                    "country": "US",
                    "email": "rclark@telecomservicebureau.com",
                    "firstname": "Easy Wireless Store",
                    "lastname": "21",
                    "postcode": "74848",
                    "region": "OK",
                    "telephone": ".",
                },
            },
            "items": [
                {"price": "3.1900", "qty": "2.0000", "sku": "AKYOC6742HPCSAAS221NP"},
                {"price": "4.9500", "qty": "2.0000", "sku": "TSLG40211"},
                {"price": "4.9500", "qty": "2.0000", "sku": "TSLG40214"},
                {"price": "2.9500", "qty": "1.0000", "sku": "1HBTFM-MOTXT1924-PKBK"},
                {"price": "4.1500", "qty": "1.0000", "sku": "1STT-MOTXT1924-BLBK"},
                {"price": "3.2500", "qty": "1.0000", "sku": "1HBTFM-SAMGA6-BKBK"},
                {"price": "2.5000", "qty": "2.0000", "sku": "STT-ZTEN9137-BLBK"},
                {"price": "2.5000", "qty": "2.0000", "sku": "STT-ZTEN9137-SLBK"},
                {"price": "1.5000", "qty": "1.0000", "sku": "1AM3H-ZTEN9517-BKBK"},
                {"price": "1.5000", "qty": "1.0000", "sku": "1AM3H-ZTEN9517-GRBK"},
            ],
        }

        variables = {
            "txType": "order",
            "source": "MAGE2SQS",
            "srcId": "2010071552",
            "data": json.dumps(data),
        }

        payload = {"mutation": mutation, "variables": variables}

        response = self.interface.interface_graphql(**payload)
        logger.info(response)

    @unittest.skip("demonstrating skipping")
    def test_graphql_insertitemreceipt(self):
        mutation = """
            mutation InsertTransaction($txType: String!, $source: String!, $srcId: String!, $data: String!) {
                insertTransaction(txType: $txType, source: $source, srcId: $srcId, data: $data) {
                    transaction {
                        __typename
                        id
                        source
                        srcId
                        tgtId
                        txType
                        ... on ItemreceiptType {
                            data{
                                internalId
                                tgtId
                                key
                                orderDate
                                refNo
                                status
                                tranIds
                                updateDate
                                shipTo{
                                    address
                                    city
                                    contact
                                    countryCode
                                    name
                                    shipping
                                    state
                                    zip
                                }
                                items{
                                    internalId
                                    itemNo
                                    qty
                                }
                            }
                        }
                        createdAt
                        updatedAt
                        txNote
                        txStatus
                    }
                }
            }
        """

        data = {
            "internal_id": "4410288",
            "items": [
                {"internal_id": "128207", "item_no": "1BOLT-LGSTL4-BKBK", "qty": 1},
                {"internal_id": "118302", "item_no": "1BOLT-MOTXT1924-BKBK", "qty": 1},
                {"internal_id": "128607", "item_no": "1GLSHD-LGSTL4-BLK", "qty": 2},
                {"internal_id": "127507", "item_no": "1IONC-LGSTL4-BKSM", "qty": 4},
                {"internal_id": "124307", "item_no": "BOLT-LGHM2-BKBK", "qty": 1},
                {"internal_id": "118202", "item_no": "BOLT-MOTXT1921-BKBK", "qty": 1},
                {"internal_id": "132107", "item_no": "GLSHD-LGHM2-BLK", "qty": 2},
            ],
            "key": "SO56374",
            "order_date": "2019-04-03 17:56:24",
            "ref_no": ["PO91541"],
            "ship_to": {
                "address": "9722 Topanga Canyon Blvd",
                "city": "Chatsworth",
                "contact": "Albert Bitar",
                "country_code": "US",
                "name": "MyCoolCell, LLC",
                "shipping": "PICKUP",
                "state": "CA",
                "zip": "91311",
            },
            "status": "Completed",
            "tran_ids": ["PO91541"],
            "update_date": "2019-04-03 17:56:24",
        }

        variables = {
            "txType": "itemreceipt",
            "source": "S3-NS",
            "srcId": "SO56374",
            "data": json.dumps(data),
        }

        payload = {"mutation": mutation, "variables": variables}
        response = self.interface.interface_graphql(**payload)
        logger.info(response)

    @unittest.skip("demonstrating skipping")
    def test_graphql_getorder(self):
        logger.info(sys._getframe().f_code.co_name)

        query = """
        query getTransaction($source: String!, $srcId: String!, $txType: String!){
            transaction(source: $source, srcId: $srcId, txType: $txType) {
                __typename
                id
                source
                srcId
                tgtId
                txType
                ... on OrderType {
                    data{
                        orderStatus
                        addresses{
                            billto{
                                address
                                city
                                company
                                country
                                email
                                firstname
                                lastname
                                postcode
                                region
                                telephone
                            }
                            shipto{
                                address
                                city
                                company
                                country
                                email
                                firstname
                                lastname
                                postcode
                                region
                                telephone
                            }
                        }
                        items{
                            price
                            qty
                            sku
                        }
                    }
                    history{
                        tgtId
                        updatedAt
                        orderStatus
                        addresses{
                            billto{
                                address
                                city
                                company
                                country
                                email
                                firstname
                                lastname
                                postcode
                                region
                                telephone
                            }
                            shipto{
                                address
                                city
                                company
                                country
                                email
                                firstname
                                lastname
                                postcode
                                region
                                telephone
                            }
                        }
                        items{
                            price
                            qty
                            sku
                        }
                    }
                }
                createdAt
                updatedAt
                txNote
                txStatus
            }
        }
        """

        variables = {"source": "MAGE2SQS", "srcId": "2010071552", "txType": "order"}

        payload = {"query": query, "variables": variables}

        response = self.interface.interface_graphql(**payload)
        logger.info(response)

    @unittest.skip("demonstrating skipping")
    def test_graphql_getitemreceipt(self):
        logger.info(sys._getframe().f_code.co_name)

        query = """
        query getTransaction($source: String!, $srcId: String!, $txType: String!){
            transaction(source: $source, srcId: $srcId, txType: $txType) {
                __typename
                id
                source
                srcId
                tgtId
                txType
                ... on ItemreceiptType {
                    data{
                        internalId
                        key
                        orderDate
                        refNo
                        status
                        tranIds
                        updateDate
                        shipTo{
                            address
                            city
                            contact
                            countryCode
                            name
                            shipping
                            state
                            zip
                        }
                        items{
                            internalId
                            itemNo
                            qty
                        }
                    }
                }
                createdAt
                updatedAt
                txNote
                txStatus
            }
        }
        """

        variables = {"source": "S3-NS", "srcId": "SO56374", "txType": "itemreceipt"}

        payload = {"query": query, "variables": variables}

        response = self.interface.interface_graphql(**payload)
        logger.info(response)

    # @unittest.skip("demonstrating skipping")
    def test_graphql_updatetransactionstatus(self):
        mutation = """
            mutation UpdateTransactionStatus(
                $source: String!, 
                $id: String!, 
                $tgtId: String!, 
                $txNote: String!, 
                $txStatus: String!
            ) {
                updateTransactionStatus(
                    source: $source, 
                    id: $id, 
                    tgtId: $tgtId, 
                    txNote: $txNote, 
                    txStatus: $txStatus
                ) {
                    status
                }
            }
        """

        variables = {
            "source": "MAGE2SQS",
            "id": "d5b5038a-ad24-11eb-8acd-0242ac120002",
            "tgtId": "4662959",
            "txNote": "DataWald -> S3-NS",
            "txStatus": "S",
        }

        payload = {"mutation": mutation, "variables": variables}

        response = self.interface.interface_graphql(**payload)
        logger.info(response)

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
                    "updatedAt": "2021-04-19T12:24:04.235901+0000",
                }
            ],
        }

        payload = {"mutation": mutation, "variables": variables}

        response = self.interface.interface_graphql(**payload)
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
                    "updatedAt": "2021-04-19T12:24:04.235901+0000",
                }
            ],
        }

        payload = {"mutation": mutation, "variables": variables}

        response = self.interface.interface_graphql(**payload)
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

        variables = {
            "table": "transaction",
            "source": "MAGE2SQS",
            "id": "594efb4a-91d3-11eb-b0eb-d8f2cab5f526",
        }

        payload = {"query": query, "variables": variables}

        response = self.interface.interface_graphql(**payload)
        logger.info(response)

    @unittest.skip("demonstrating skipping")
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

        response = self.interface.interface_graphql(**payload)
        logger.info(response)

    @unittest.skip("demonstrating skipping")
    def test_graphql_getsynctask(self):
        logger.info(sys._getframe().f_code.co_name)

        query = """
            query getSyncTask($task: String!, $id: String!) {
                syncTask(task: $task, id: $id) {
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
        """

        variables = {"task": "syncOrder", "id": "12118427224148808171"}

        payload = {"query": query, "variables": variables}

        response = self.interface.interface_graphql(**payload)
        logger.info(response)
    

if __name__ == "__main__":
    unittest.main()
