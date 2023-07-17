import boto3
import base64
import os

KMS = "kms"


def decrypt(encrypted_value):
    """
    Gives decoded and decrypted value 
    :param encrypted_value: kms encrypted values from environment variables
    :return: decrypted values
    """
    session = boto3.session.Session()

    kms = session.client(KMS)

    encrypted_id = encrypted_value
    binary_data = base64.b64decode(encrypted_id)
    meta = kms.decrypt(CiphertextBlob=binary_data)
    plaintext = meta[u'Plaintext']
    return plaintext.decode()
