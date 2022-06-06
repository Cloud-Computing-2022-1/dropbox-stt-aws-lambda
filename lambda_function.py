import os
import sys
import boto3
import pymysql
import subprocess
import requests
import urllib.parse

s3 = boto3.client("s3")


def run_command(command: str):
    """
    OS 명령어 실행하는 함수
    """
    return subprocess.run(command.split(" "), stdout=subprocess.PIPE).stdout.decode("utf-8")


def download_file(bucket: str, key: str, path: str):
    """
    S3에 업로드된 mp4 파일을 실제 파일로 다운로드하는 함수
    """
    with open(path, "wb") as f:
        s3.download_fileobj(bucket, key, f)


def split_file(path: str, duration: int):
    """
    지정된 파일을 ffmpeg를 통해 여러 파일로 잘라내는 함수
    """
    if duration > 60:
        raise Exception("duration must be less than 60")
        
    print(run_command("/opt/ffmpeg -i {path} -c copy -map 0 -segment_time 00:00:{duration} -f segment /tmp/output%03d.mp4".format(path=path, duration=duration)))


def extract_text(path):
    """
    Naver CLOVA API를 호출해 stt 기능 구현한 함수 
    """
    print("HIHIHIHIHIHIHIHIHIHI")
    url = "https://naveropenapi.apigw.ntruss.com/recog/v1/stt?lang=Kor"
    headers = {
        "X-NCP-APIGW-API-KEY-ID": "***",
        "X-NCP-APIGW-API-KEY": "***",
        "Content-Type": "application/octet-stream",
    }
    data = open(path, "rb")
    response = requests.post(url, headers=headers, data=data)
    if response.status_code != 200:
        print("[ ERROR ] status_code :", response.status_code)
        print(response.text)
        sys.exit(-1)
    return response.json().get("text")


def get_full_text(path):
    """
    30초 단위로 쪼개진 output*.mp4 마다 stt API 사용
    """
    
    split_file(path, 30)
    
    full_text = ""

    for i in range(0, 1000):
        output_path = "/tmp/output%03d.mp4" % i
        if not os.path.isfile(output_path):
            break
        
        full_text += extract_text(output_path)
        
    return full_text


def save_video_script(key, script):
    """
    STT 결과를 AWS RDS에 저장
    """
    try:
        connection = pymysql.connect(
            host="***",
            user="***",
            password="***",
            db="***",
        )
    except pymysql.MySQLError as e:
        print("[ ERROR ] MySQL Connection Error")
        print(e)
        sys.exit(-1)
    
    with connection.cursor() as cursor:
        print(
            "UPDATE cloudstorage_fileinfo SET `script` = '{script}' WHERE `key`='{key}'".format(
                script=script,
                key=key,
            )
        )
        cursor.execute(
            "UPDATE cloudstorage_fileinfo SET `script` = '{script}' WHERE `key`='{key}'".format(
                script=script,
                key=key,
            )
        )
    
    connection.commit()
    

def lambda_handler(event, context):
    """
    트리거 호출시 실행되는 함수
    """
    # print("[ DEBUG ] event :", json.dumps(event, indent=2))
    
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = urllib.parse.unquote_plus(event["Records"][0]["s3"]["object"]["key"], encoding="utf-8")
    
    download_file(bucket, key, "/tmp/input.mp4")
    # print("[ DEBUG ] $", run_command("ls -al /tmp/input.mp4"))
    

    text = get_full_text("/tmp/input.mp4") 
    save_video_script(key, text)
    
    print("[ DEBUG ] text :", text)
    
    