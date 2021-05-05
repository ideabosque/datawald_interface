#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function

__author__ = "bibow"

import os, uuid, boto3, traceback
from datetime import datetime
from time import sleep
from silvaengine_utility import Utility
from datawald_model.models import BaseModel, SyncTaskModel, EntityMap, TransactionModel


class Control(object):

    # sqs = boto3.client("sqs")
    # aws_lambda = boto3.client("lambda")

    logger = None

    @classmethod
    def entity_model(cls, table):
        _entity_model = {"transaction": TransactionModel}
        return _entity_model.get(table)

    @classmethod
    def get_task(cls, table, source, id):
        entity_model = cls.entity_model(table)
        assert entity_model is not None, f"The table ({table}) is not supported."

        entity = entity_model.get(source, id)

        return {
            "source": source,
            "id": id,
            "task_status": entity.tx_status,
            "task_note": entity.tx_note,
            "updated_at": entity.updated_at,
            "ready": 1 if entity.tx_status != "N" else 0,
        }

    @classmethod
    def get_cut_date(cls, source, task):
        # cut_date = os.environ["DEFAULTCUTDATE"]
        offset = 0
        sync_statuses = ["Completed", "Fail", "Incompleted", "Processing"]
        sync_tasks = [
            sync_task
            for sync_task in SyncTaskModel.task_source_index.query(
                task,
                SyncTaskModel.source == source,
                SyncTaskModel.sync_status.is_in(*sync_statuses),
            )
        ]

        if len(sync_tasks) > 0:
            last_sync_task = max(
                sync_tasks,
                key=lambda sync_task: (sync_task.cut_date, int(sync_task.offset)),
            )
            id = last_sync_task.id
            cut_date = last_sync_task.cut_date
            offset = int(last_sync_task.offset)

            # Flsuh Sync Task Table by frontend and task.
            cls.flush_sync_task(task, source, id)
        return cut_date, offset

    @classmethod
    def flush_sync_task(cls, task, source, id):
        for sync_task in SyncTaskModel.task_source_index.query(
            task, SyncTaskModel.source == source, SyncTaskModel.id != id
        ):
            sync_task.delete(SyncTaskModel.id != id)

    @classmethod
    def get_sync_task(cls, task, id):
        return SyncTaskModel.get(task, id)

    @classmethod
    def insert_sync_task(cls, **sync_task):
        id = str(uuid.uuid1().int >> 64)
        sync_task_model = SyncTaskModel(
            sync_task["task"],
            id,
            **{
                "source": sync_task["source"],
                "target": sync_task["target"],
                "table": sync_task["table"],
                "cut_date": sync_task["cut_date"],
                "start_date": datetime.utcnow(),
                "end_date": datetime.utcnow(),
                "offset": sync_task.get("offset", 0),
                "sync_note": f"Process task ({sync_task['task']}) for source ({sync_task['source']}).",
                "sync_status": "Processing",
                "entities": [EntityMap(**entity) for entity in sync_task["entities"]],
            },
        )
        sync_task_model.save()

        # if len(sync_task_model.entities) > 0:
        #     queue_name = (
        #         f"{sync_task['source']}_{sync_task['target']}_{sync_task['table']}_{id}"[
        #             :75
        #         ]
        #         + ".fifo"
        #     )
        #     Control.dispatch_sync_task(
        #         self.logger,
        #         sync_task["task"],
        #         sync_task["target"],
        #         queue_name,
        #         sync_task_model.entities,
        #     )

        return SyncTaskModel.get(sync_task["task"], id)

    @classmethod
    def update_sync_task(cls, task, id, entities):
        sync_status = "Completed"
        if len(list(filter(lambda x: x["task_status"] == "F", entities))) > 0:
            sync_status = "Fail"
        if len(list(filter(lambda x: x["task_status"] == "?", entities))) > 0:
            sync_status = "Incompleted"

        sync_task_model = SyncTaskModel.get(task, id)
        sync_task_model.update(
            actions=[
                SyncTaskModel.sync_status.set(sync_status),
                SyncTaskModel.end_date.set(datetime.utcnow()),
                SyncTaskModel.entities.set(
                    [EntityMap(**entity) for entity in entities]
                ),
            ]
        )

        return SyncTaskModel.get(task, id)

    # @classmethod
    # def dispatch_sync_task(cls, logger, task, target, queue_name, entities):
    #     max_task_agents = int(os.environ.get("MAXTASKAGENTS", "1"))
    #     function_name = os.environ["AGENTTASKARN"]

    #     try:
    #         task_queue = cls.sqs.create_queue(
    #             QueueName=queue_name,
    #             Attributes={"FifoQueue": "true", "ContentBasedDeduplication": "true"},
    #         )

    #         for entity in entities:
    #             if entity.tx_status != "N":
    #                 continue

    #             task_queue.send_message(
    #                 MessageBody=Utility.json_dumps(
    #                     {"source": entity.source, "id": entity.id}
    #                 ),
    #                 MessageGroupId=id,
    #             )

    #         while max_task_agents:
    #             cls.aws_lambda.invoke(
    #                 FunctionName=function_name,
    #                 InvocationType="Event",
    #                 Payload=Utility.json_dumps(
    #                     {"endpoint_id": target, "queue_name": queue_name, "funct": task}
    #                 ),
    #             )
    #             max_task_agents -= 1
    #             sleep(1)
    #     except Exception:
    #         log = traceback.format_exc()
    #         logger.exception(log)
    #         raise
