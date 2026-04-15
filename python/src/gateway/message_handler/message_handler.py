from common import message_protocol

import uuid

class MessageHandler:

    def __init__(self):
        self.messages_id = uuid.uuid4()
    
    def serialize_data_message(self, message):
        [fruit, amount] = message
        return message_protocol.internal.serialize([self.messages_id, fruit, amount])

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize([self.messages_id])

    def deserialize_result_message(self, message):
        fields = message_protocol.internal.deserialize(message)
        return fields
