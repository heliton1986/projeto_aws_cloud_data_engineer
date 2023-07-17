import datetime
import logging
import os
import boto3
from botocore.exceptions import ClientError

session = boto3.Session(profile_name="heliton")

s3 = session.client('s3')

date = datetime.datetime.now()

def subir_arquivo(bucket, object_name=None):

    categorias = ['Movies', 'Series']

    try:

        for i in range(len(categorias)):

            fazer_upload(categorias[i], object_name, bucket)

    except ClientError as e:

        logging.error(e)

        return False

    finally:

        print("\nExecução finalizada")

    return True


def fazer_upload(categorias, object_name, bucket):

    if object_name is None:
        object_name = os.path.basename(
            f"{(categorias).lower()}.csv")


    file = f"{(categorias).lower()}.csv"

    outpath = "RAW/Local/CSV/{}/{}/{}/{}/{}".format(
        categorias, date.year, date.month, date.day, object_name)

    print(f'\nIniciando carregamento em {outpath}')

    response = s3.upload_file(file, bucket, outpath)

    print('Carregamento finalizado....')

    return True

if __name__ == '__main__':

    subir_arquivo("data-lake-heliton")
