#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function

__author__ = "bibow"

import uuid
from datawald_model.models import TransactionModel
from datetime import datetime
from silvaengine_utility import Utility


class Transaction(object):

    type_class_module = {
        "order_object_types_module": None,
        "itemreceipt_object_types_module": None,
    }

    logger = None

    @classmethod
    def get_transaction(cls, source, src_id, tx_type):
        type_class = cls.type_class(tx_type)
        count = TransactionModel.source_src_id_index.count(
            source,
            TransactionModel.src_id == src_id,
            TransactionModel.tx_type == tx_type,
        )
        if count == 0:
            return type_class()

        results = TransactionModel.source_src_id_index.query(
            source,
            TransactionModel.src_id == src_id,
            TransactionModel.tx_type == tx_type,
        )
        return type_class(
            **Utility.json_loads(
                Utility.json_dumps(results.next().__dict__["attribute_values"])
            )
        )

    @classmethod
    def set_type_class_module(cls, **tx_type_class_module):
        for k, v in tx_type_class_module.items():
            cls.type_class_module[k] = v

    @classmethod
    def type_class(cls, tx_type):
        order_object_types_module = __import__(
            cls.type_class_module["order_object_types_module"]
        )
        itemreceipt_object_types_module = __import__(
            cls.type_class_module["itemreceipt_object_types_module"]
        )

        _type_class = {
            "order": getattr(order_object_types_module, "OrderType"),
            "itemreceipt": getattr(itemreceipt_object_types_module, "ItemreceiptType"),
        }
        return _type_class.get(tx_type)

    @classmethod
    def insert_transaction(cls, transaction, tx_type=None):
        tx_status = "N"

        _id = str(uuid.uuid1())
        created_at = datetime.utcnow()
        history = []

        count = TransactionModel.source_src_id_index.count(
            transaction["source"],
            TransactionModel.src_id == transaction["src_id"],
            TransactionModel.tx_type == tx_type,
        )

        if count >= 1:
            results = TransactionModel.source_src_id_index.query(
                transaction["source"],
                TransactionModel.src_id == transaction["src_id"],
                TransactionModel.tx_type == tx_type,
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
                            f"No update {tx_type}: {transaction['source']}/{transaction['src_id']}"
                        ),
                        TransactionModel.tx_status.set(tx_status),
                    ]
                )

        cls.logger.info(transaction)
        transaction_model = TransactionModel(
            transaction["source"],
            _id,
            **{
                "src_id": transaction["src_id"],
                "tx_type": tx_type,
                "data": transaction["data"],
                "history": history,
                "created_at": created_at,
                "updated_at": datetime.utcnow(),
                "tx_note": f"{transaction['source']} -> DataWald",
                "tx_status": tx_status,
            },
        )

        return transaction_model.save()

    @classmethod
    def update_transaction_status(cls, source, id, transaction_status):
        transaction_model = TransactionModel.get(source, id)

        return transaction_model.update(
            actions=[
                TransactionModel.tgt_id.set(transaction_status["tgt_id"]),
                TransactionModel.updated_at.set(datetime.utcnow()),
                TransactionModel.tx_note.set(transaction_status["tx_note"]),
                TransactionModel.tx_status.set(transaction_status["tx_status"]),
            ]
        )