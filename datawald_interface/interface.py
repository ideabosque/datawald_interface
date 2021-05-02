#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function

__author__ = "bibow"

import uuid, json, traceback
from datetime import datetime, date
from decimal import Decimal
from datawald_model.models import BaseModel, TransactionModel
from pynamodb.attributes import (
    MapAttribute,
    ListAttribute,
    UnicodeAttribute,
    NumberAttribute,
    UnicodeSetAttribute,
    UTCDateTimeAttribute,
)
from graphene import Field, ObjectType, Schema, Mutation, Boolean, String
from silvaengine_utility import Utility


class Interface(object):
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

    @property
    def type_class(self):
        order_object_types_module = __import__(
            self.setting.get(
                "order_object_types_module", "datawald_model.order_object_types"
            )
        )
        itemreceipt_object_types_module = __import__(
            self.setting.get(
                "itemreceipt_object_types_module",
                "datawald_model.itemreceipt_object_types",
            )
        )

        return {
            "order": getattr(order_object_types_module, "OrderType"),
            "itemreceipt": getattr(itemreceipt_object_types_module, "ItemReceiptType"),
        }

    def _get_transaction(self, source, src_id, tx_type):
        count = TransactionModel.source_src_id_index.count(
            source, TransactionModel.src_id == src_id, TransactionModel.type == tx_type
        )
        if count >= 0:
            results = TransactionModel.source_src_id_index.query(
                source,
                TransactionModel.src_id == src_id,
                TransactionModel.type == tx_type,
            )
            return results.next()
        else:
            return None

    def get_transaction(self, source, src_id, tx_type=None):
        transaction = self._get_transaction(source, src_id, tx_type)
        type_class = self.type_class[tx_type]

        if transaction is None:
            return None

        return type_class(
            **Utility.json_loads(
                Utility.json_dumps(transaction.__dict__["attribute_values"])
            )
        )

    def insert_transaction(self, transaction, tx_type=None):
        tx_status = "N"

        _id = str(uuid.uuid1())
        created_at = datetime.utcnow()
        history = []

        count = TransactionModel.source_src_id_index.count(
            transaction["source"],
            TransactionModel.src_id == transaction["src_id"],
            TransactionModel.type == tx_type,
        )

        if count >= 1:
            results = TransactionModel.source_src_id_index.query(
                transaction["source"],
                TransactionModel.src_id == transaction["src_id"],
                TransactionModel.type == tx_type,
            )
            _transaction = results.next()
            _id = _transaction.id
            created_at = _transaction.created_at

            data = _transaction.data.__dict__["attribute_values"]
            changed_values = [
                {k: v} for k, v in transaction["data"].items() if data[k] != v
            ]
            if len(changed_values) > 0:
                _transaction.data.tgt_id = _transaction.tgt_id
                _transaction.data.updated_at = _transaction.updated_at.strftime(
                    "%Y-%m-%dT%H:%M:%S"
                )
                history = _transaction.history + [_transaction.data]
            else:
                tx_status = "I"
                return _transaction.update(
                    actions=[
                        TransactionModel.updated_at.set(datetime.utcnow()),
                        TransactionModel.tx_note.set(
                            "No update {tx_type}: {source}/{src_id}".format(
                                tx_type=tx_type,
                                source=transaction["source"],
                                src_id=transaction["src_id"],
                            )
                        ),
                        TransactionModel.tx_status.set(tx_status),
                    ]
                )
        self.logger.info(transaction)
        transaction_model = TransactionModel(
            transaction["source"],
            _id,
            **{
                "src_id": transaction["src_id"],
                "type": tx_type,
                "data": transaction["data"],
                "history": history,
                "created_at": created_at,
                "updated_at": datetime.utcnow(),
                "tx_note": "{source} -> DataWald".format(source=transaction["source"]),
                "tx_status": tx_status,
            }
        )

        return transaction_model.save()

    def update_transaction_status(self, source, id, transaction_status):
        transaction_model = TransactionModel.get(source, id)

        return transaction_model.update(
            actions=[
                TransactionModel.tgt_id.set(transaction_status["tgt_id"]),
                TransactionModel.updated_at.set(datetime.utcnow()),
                TransactionModel.tx_note.set(transaction_status["tx_note"]),
                TransactionModel.tx_status.set(transaction_status["tx_status"]),
            ]
        )

    def interface_graphql(self, **params):
        outer = self

        class Query(ObjectType):
            order = Field(
                self.type_class["order"],
                source=String(required=True),
                src_id=String(required=True),
            )
            itemreceipt = Field(
                self.type_class["itemreceipt"],
                source=String(required=True),
                src_id=String(required=True),
            )

            def resolve_order(self, info, **kwargs):
                return outer.get_transaction(
                    kwargs.get("source"), kwargs.get("src_id"), tx_type="order"
                )

            def resolve_itemreceipt(self, info, **kwargs):
                return outer.get_transaction(
                    kwargs.get("source"), kwargs.get("src_id"), tx_type="itemreceipt"
                )

        ## Mutation ##

        class InsertOrder(Mutation):
            order = Field(self.type_class["order"])

            class Arguments:
                source = String(required=True)
                src_id = String(required=True)
                data = String(required=True)

            @staticmethod
            def mutate(root, info, **kwargs):
                try:
                    self.insert_transaction(
                        {
                            "source": kwargs.get("source"),
                            "src_id": kwargs.get("src_id"),
                            "data": json.loads(kwargs.get("data")),
                        },
                        tx_type="order",
                    )
                    order = self.get_transaction(
                        kwargs.get("source"), kwargs.get("src_id"), "order"
                    )
                except Exception:
                    log = traceback.format_exc()
                    self.logger.exception(log)
                    raise

                return InsertOrder(order=order)

        class InsertItemReceipt(Mutation):
            itemreceipt = Field(self.type_class["itemreceipt"])

            class Arguments:
                source = String(required=True)
                src_id = String(required=True)
                data = String(required=True)

            @staticmethod
            def mutate(root, info, **kwargs):
                try:
                    self.insert_transaction(
                        {
                            "source": kwargs.get("source"),
                            "src_id": kwargs.get("src_id"),
                            "data": json.loads(kwargs.get("data")),
                        },
                        tx_type="itemreceipt",
                    )
                    itemreceipt = self.get_transaction(
                        kwargs.get("source"),
                        kwargs.get("src_id"),
                        "itemreceipt",
                    )
                except Exception:
                    log = traceback.format_exc()
                    self.logger.exception(log)
                    raise

                return InsertItemReceipt(itemreceipt=itemreceipt)

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
                    self.update_transaction_status(
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
                    self.logger.exception(log)
                    raise

                return UpdateTransactionStatus(status=status)

        class Mutations(ObjectType):
            insert_order = InsertOrder.Field()
            insert_itemreceipt = InsertItemReceipt.Field()
            update_transaction_status = UpdateTransactionStatus.Field()

        ## Mutation ##

        schema = Schema(
            query=Query,
            mutation=Mutations,
            types=[value for value in self.type_class.values()],
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
