#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function

__author__ = "bibow"

import json, traceback, logging
from graphene import (
    Field,
    ObjectType,
    Schema,
    Mutation,
    Boolean,
    String,
    DateTime,
    Int,
    List,
)
from graphdoc import to_doc
from silvaengine_utility import Utility
from datawald_model.common_object_types import TransactionType
from datawald_model.control_object_types import (
    TaskType,
    CutDateType,
    SyncTaskType,
    EntityInputType,
)
from .transaction import Transaction
from .control import Control


def schema_init(**kwargs):
    Transaction.set_type_class_module(**kwargs.get("type_class_module"))
    Transaction.logger = kwargs.get("logger")
    Control.logger = kwargs.get("logger")


def type_class():
    return [
        Transaction.type_class("order"),
        Transaction.type_class("itemreceipt"),
        TaskType,
        CutDateType,
        SyncTaskType,
    ]


class Query(ObjectType):
    transaction = Field(
        TransactionType,
        required=True,
        source=String(required=True),
        src_id=String(required=True),
        tx_type=String(required=True),
    )

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

    def resolve_transaction(self, info, **kwargs):
        return Transaction.get_transaction(
            kwargs.get("source"),
            kwargs.get("src_id"),
            tx_type=kwargs.get("tx_type"),
        )

    def resolve_task(self, info, **kwargs):
        task = Control.get_task(
            kwargs.get("table"), kwargs.get("source"), kwargs.get("id")
        )
        return TaskType(**task)

    def resolve_cut_date(self, info, **kwargs):
        cut_date, offset = Control.get_cut_date(
            kwargs.get("source"), kwargs.get("task")
        )
        return CutDateType(cut_date=cut_date, offset=offset)

    def resolve_sync_task(self, info, **kwargs):
        sync_task_model = Control.get_sync_task(kwargs.get("task"), kwargs.get("id"))
        return SyncTaskType(**sync_task_model.__dict__["attribute_values"])


class InsertTransaction(Mutation):
    transaction = Field(TransactionType)

    class Arguments:
        tx_type = String(required=True)
        source = String(required=True)
        src_id = String(required=True)
        data = String(required=True)

    @staticmethod
    def mutate(root, info, **kwargs):
        try:
            Transaction.insert_transaction(
                {
                    "source": kwargs.get("source"),
                    "src_id": kwargs.get("src_id"),
                    "data": json.loads(kwargs.get("data")),
                },
                tx_type=kwargs.get("tx_type"),
            )
            transaction = Transaction.get_transaction(
                kwargs.get("source"),
                kwargs.get("src_id"),
                kwargs.get("tx_type"),
            )

        except Exception:
            log = traceback.format_exc()
            info.context.get("logger").exception(log)
            raise

        return InsertTransaction(transaction=transaction)


class UpdateTransactionStatus(Mutation):
    status = Boolean()

    class Arguments:
        source = String(required=True)
        id = String(required=True)
        tgt_id = String(required=True)
        tx_note = String(required=True)
        tx_status = String(required=True)

    @staticmethod
    def mutate(root, info, **kwargs):
        try:
            Transaction.update_transaction_status(
                kwargs.get("source"),
                kwargs.get("id"),
                {
                    "tgt_id": kwargs.get("tgt_id"),
                    "tx_note": kwargs.get("tx_note"),
                    "tx_status": kwargs.get("tx_status"),
                },
            )
            status = True
        except Exception:
            log = traceback.format_exc()
            info.context.get("logger").exception(log)
            raise

        return UpdateTransactionStatus(status=status)


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
            sync_task_model = Control.insert_sync_task(**kwargs)
            sync_task = SyncTaskType(**sync_task_model.__dict__["attribute_values"])

        except Exception:
            log = traceback.format_exc()
            info.context.get("logger").exception(log)
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
            sync_task_model = Control.update_sync_task(**kwargs)
            sync_task = SyncTaskType(**sync_task_model.__dict__["attribute_values"])

        except Exception:
            log = traceback.format_exc()
            info.context.get("logger").exception(log)
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
            sync_task_model = Control.update_sync_task(**kwargs)
            sync_task_model.delete()
            ok = True

        except Exception:
            log = traceback.format_exc()
            info.context.get("logger").exception(log)
            raise

        return DeleteSyncTask(ok=ok)


class Mutations(ObjectType):
    insert_transaction = InsertTransaction.Field()
    update_transaction_status = UpdateTransactionStatus.Field()
    insert_sync_task = InsertSyncTask.Field()
    update_sync_task = UpdateSyncTask.Field()
    delete_sync_task = DeleteSyncTask.Field()


def graphql_schema_doc():
    logger = logging.getLogger()
    schema_init(
        **{
            "type_class_module": {
                "order_object_types_module": "datawald_model.order_object_types",
                "itemreceipt_object_types_module": "datawald_model.itemreceipt_object_types",
            },
            "logger": logger,
        }
    )

    schema = Schema(
        query=Query,
        mutation=Mutations,
        types=type_class(),
    )
    return to_doc(schema)