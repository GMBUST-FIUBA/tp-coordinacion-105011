from common import message_protocol

import uuid

UUID_POSITION = 0

class MessageHandler:

    def __init__(self):
        self.messages_id = str(uuid.uuid4())
    
    def serialize_data_message(self, message):
        [fruit, amount] = message
        return message_protocol.internal.serialize([self.messages_id, fruit, amount])

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize([self.messages_id])

    def deserialize_result_message(self, message):
        fields = message_protocol.internal.deserialize(message)
        if fields[UUID_POSITION] == self.messages_id:
            return fields[UUID_POSITION + 1:]
        else:
            return None
