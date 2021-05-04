#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function

__author__ = "bibow"

import os, uuid, boto3, traceback
from datetime import datetime
from time import sleep
from silvaengine_utility import Utility
from datawald_model.models import BaseModel, SyncTaskModel, EntityMap, TransactionModel
from datawald_model.control_object_types import (
    TaskType,
    CutDateType,
    SyncTaskType,
    EntityInputType,
)
from graphene import Field, ObjectType, Schema, Mutation, String, DateTime, Int, Boolean, List


class Control(object):

    # sqs = boto3.client("sqs")
    # aws_lambda = boto3.client("lambda")

    def __init__(self, logger, **setting):
        self.logger = logger
        self.setting = setting
        if (
            setting.get("region_name")
            and setting.get("aws_access_key_id")
            and setting.get("aws_secret_access_key")
        ):
            BaseModel.Meta.region = setting.get("region_name")
            BaseModel.Meta.aws_access_key_id = setting.get("aws_access_key_id")
            BaseModel.Meta.aws_secret_access_key = setting.get("aws_secret_access_key")

    def entity_model(self, table):
        _entity_model = {"transaction": TransactionModel}
        return _entity_model.get(table)

    # Add GraphQL Query.
    def get_task(self, table, source, id):
        entity_model = self.entity_model(table)
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

    # Add GraphQL Query.
    def get_cut_date(self, source, task):
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
            self.flush_sync_task(task, source, id)
        return cut_date, offset

    def flush_sync_task(self, task, source, id):
        for sync_task in SyncTaskModel.task_source_index.query(
            task, SyncTaskModel.source == source, SyncTaskModel.id != id
        ):
            sync_task.delete(SyncTaskModel.id != id)

    # Add GraphQL Mutation.
    def insert_sync_task(self, **sync_task):
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

    # Add GraphQL Mutation.
    def update_sync_task(self, task, id, entities):
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

    # Add GraphQL Mutation.
    def del_sync_task(self, task, id):
        sync_task = SyncTaskModel.get(task, id)
        sync_task.delete()

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

    def control_graphql(self, **params):
        outer = self

        class Query(ObjectType):
            task = Field(
                TaskType,
                table=String(required=True),
                source=String(required=True),
                id=String(required=True),
            )
            cut_date = Field(
                CutDateType, source=String(required=True), task=String(required=True)
            )
            sync_task = Field(
                SyncTaskType,
                task=String(required=True),
                id=String(required=True),
            )

            def resolve_task(self, info, **kwargs):
                task = outer.get_task(
                    kwargs.get("table"), kwargs.get("source"), kwargs.get("id")
                )
                return TaskType(**task)

            def resolve_cut_date(self, info, **kwargs):
                cut_date, offset = outer.get_cut_date(
                    kwargs.get("source"), kwargs.get("task")
                )
                return CutDateType(cut_date=cut_date, offset=offset)

            def resolve_sync_task(self, info, **kwargs):
                sync_task_model = SyncTaskModel.get(
                    kwargs.get("task"), kwargs.get("id")
                )
                return SyncTaskType(**sync_task_model.__dict__["attribute_values"])

        ## Mutation ##

        class InsertSyncTask(Mutation):
            sync_task = Field(SyncTaskType)

            class Arguments:
                task = String(required=True)
                source = String(required=True)
                target = String(required=True)
                table = String(required=True)
                cut_date = DateTime(required=True)
                offset = Int()
                entities = List(EntityInputType)

            @staticmethod
            def mutate(root, info, **kwargs):
                try:
                    sync_task_model = outer.insert_sync_task(**kwargs)
                    sync_task = SyncTaskType(
                        **sync_task_model.__dict__["attribute_values"]
                    )

                except Exception:
                    log = traceback.format_exc()
                    self.logger.exception(log)
                    raise

                return InsertSyncTask(sync_task=sync_task)

        class UpdateSyncTask(Mutation):
            sync_task = Field(SyncTaskType)

            class Arguments:
                task = String(required=True)
                id = String(required=True)
                entities = List(EntityInputType)

            @staticmethod
            def mutate(root, info, **kwargs):
                try:
                    sync_task_model = outer.update_sync_task(**kwargs)
                    sync_task = SyncTaskType(
                        **sync_task_model.__dict__["attribute_values"]
                    )

                except Exception:
                    log = traceback.format_exc()
                    self.logger.exception(log)
                    raise

                return UpdateSyncTask(sync_task=sync_task)

        class DeleteSyncTask(Mutation):
            ok = Boolean()

            class Arguments:
                task = String(required=True)
                id = String(required=True)

            @staticmethod
            def mutate(root, info, **kwargs):
                try:
                    sync_task_model = outer.update_sync_task(**kwargs)
                    sync_task_model.delete()
                    ok = True

                except Exception:
                    log = traceback.format_exc()
                    self.logger.exception(log)
                    raise

                return DeleteSyncTask(ok=ok)

        class Mutations(ObjectType):
            insert_sync_task = InsertSyncTask.Field()
            update_sync_task = UpdateSyncTask.Field()
            delete_sync_task = DeleteSyncTask.Field()

        ## Mutation ##

        schema = Schema(
            query=Query,
            mutation=Mutations,
            types=[TaskType, CutDateType, SyncTaskType],
        )

        variables = params.get("variables", {})
        query = params.get("query")
        if query is not None:
            result = schema.execute(query, variable_values=variables)
        mutation = params.get("mutation")
        if mutation is not None:
            result = schema.execute(mutation, variable_values=variables)

        response = {
            "data": dict(result.data) if result.data != None else None,
        }
        if result.errors != None:
            response["errors"] = [str(error) for error in result.errors]
        return Utility.json_dumps(response)
