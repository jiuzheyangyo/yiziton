#!/usr/bin/python3
# -*- coding: UTF-8 -*-

import datetime
import json
import logging
import random
import sched
import smtplib
import time
from email.header import Header
from email.mime.text import MIMEText
from email.utils import formataddr

import pymysql
import requests
import schedule

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s [line:%(lineno)d] %(levelname)s : %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='./oa.log')

deviceId = '23248025-F3BE-499E-908E-19B3C583E367'
name = 'L2MzEeLudE1ELe1ECMrBWg=='
password = 'WodbVuAdAq4='


def get_headers():
    """
    获取请求头
    :return:
    """
    headers = {
        'content-type': "application/json; charset=utf-8",
        'User-Agent': 'M3/2.2.1 (iPhone; iOS 12.1; Scale/2.00)',
        'Connection': 'keep-alive',
        'Accept': 'application/json; charset=utf-8'
    }
    return headers.copy()


def login():
    """
    登录
    :return:
    """
    data = {
        "client": "iphone",
        "deviceCode": deviceId,
        "name": name,
        "password": password,
        "timezone": "GMT+8"
    }
    try:
        response = requests.post("http://193.112.250.216:9999/mobile_portal/api/verification/login",
                                 data=json.dumps(data),
                                 headers=get_headers())
        response_data = response.json()
        if response.status_code != 200:
            raise Exception(response_data['message'])

        if 'currentMember' not in response_data['data']:
            raise Exception("获取用户信息失败")
    except BaseException as e:
        raise Exception("登录失败:%s" % e)

    return response_data['data']


def sign(type_):
    """
    签到
    :param type_:
    :return:
    """
    try:
        data = login()
        ticket = data['ticket']

        headers = get_headers()
        data = {
            "city": "广州市",
            "continent": "",
            "country": "",
            "deviceId": deviceId,
            "fileIds": [],
            "latitude": "23.162446",
            "longitude": "113.320378",
            "nearAddress": "广州一智通供应链管理有限公司",
            "province": "广东省",
            "receiveIds": "",
            "remark": "",
            "sign": "广东省广州市天河区广州一智通供应链管理有限公司",
            "source": 2,
            "street": "陶庄路",
            "town": "天河区",
            "type": str(type_)
        }
        headers['Cookie'] = f"JSESSIONID={ticket}"
        headers['option.n_a_s'] = '1'
        response = requests.post(
            "http://193.112.250.216:9999/mobile_portal/seeyon/rest/attendance/save?&option.n_a_s=1",
            data=json.dumps(data),
            headers=headers)
        response_data = response.json()
        if response.status_code != 200:
            raise Exception(response_data['message'])
        if not response_data['success']:
            raise Exception(response_data['message'])

        update_sign_time(datetime.date.today(), type_, datetime.datetime.now())
    except BaseException as e:
        logging.error("执行任务失败：%s" % e)
        sendmail("执行任务失败", "执行sign任务失败：%s" % (e,), "1966615884@qq.com")


def update_sign_time(date_, type_, sign_time_):
    """
    更新打卡时间
    :param date_:日期
    :param type_:类型
    :param sign_time_:签到时间
    :return:
    """
    sendmail("任务完成", "执行sign任务完成[%s]：%s" % (sign_time_.strftime("%Y-%m-%d %H:%M:%S"), type_), "1966615884@qq.com")
    execute("UPDATE sign_info_ SET sign_time_ = %s WHERE date_ = %s AND type_ = %s", (sign_time_, date_, type_))


def check_sign(datetime_):
    """
    判断打卡
    :param datetime_:
    :return:
    """
    date_now_ = datetime_.date()
    hour_ = datetime_.hour
    type_ = 1
    if hour_ > 12:
        type_ = 2
    rows = execute('SELECT * from sign_info_ where date_=%s and type_=%s', (date_now_, type_))
    if len(rows) == 0:
        execute("INSERT INTO sign_info_(date_, type_) VALUES (%s,%s)", (date_now_, type_))
        return type_
    else:
        return None


def get_connect():
    """
    获取数据库连接
    :return:
    """
    return pymysql.connect(host="localhost",
                           user="zsy",
                           password="zsy123456@.",
                           port=3307,
                           database="auto_oa")


def execute(sql_str, par):
    """
    执行sql
    :param sql_str:
    :param par:
    :return:
    """
    conn = get_connect()
    if sql_str is None:
        raise Exception("参数不能为空：sql_str")
    if len(sql_str) == 0:
        raise Exception("参数不能为空：sql_str")
    try:
        cur = conn.cursor()  # 获取一个游标
        cur.execute(sql_str, par)
        data = cur.fetchall()
        conn.commit()
        return data
    except Exception as e:
        raise e
    finally:
        if cur:
            cur.close()  # 关闭游标
        if conn:
            conn.close()  # 释放数据库资源


def do_sign():
    """
    执行签到任务
    :return:
    """
    type_ = check_sign(datetime.datetime.now())
    if not type_:
        return

    delayed_ = random.randint(0, 20)
    logging.info("延时%s分钟后执行" % delayed_)
    sendmail("准备任务", "%s分钟后执行sign任务：%s" % (delayed_, type_), "1966615884@qq.com")
    s = sched.scheduler(time.time, time.sleep)
    s.enter(delayed_ * 60, 0, sign, (type_,))
    s.run()


def sendmail(subject, content, sender_addr):
    """
    发送邮件
    :param subject: 主题
    :param content: 内容
    :param sender_addr: 发送地址
    :return:
    """
    my_sender = '1966615884@qq.com'  # 发件人邮箱账号
    my_pass = 'rbaeykyslwgocaga'  # 发件人邮箱密码

    ret = True
    try:
        msg = MIMEText(content, 'plain', 'utf-8')
        msg['From'] = formataddr(["zsy", my_sender])  # 括号里的对应发件人邮箱昵称、发件人邮箱账号
        msg['To'] = formataddr(["zsy", sender_addr])  # 括号里的对应收件人邮箱昵称、收件人邮箱账号
        msg['Subject'] = subject  # 邮件的主题，也可以说是标题

        server = smtplib.SMTP_SSL("smtp.qq.com", 465)  # 发件人邮箱中的SMTP服务器，端口是25
        server.login(my_sender, my_pass)  # 括号中对应的是发件人邮箱账号、邮箱密码
        server.sendmail(my_sender, [sender_addr, ], msg.as_string())  # 括号中对应的是发件人邮箱账号、收件人邮箱账号、发送邮件
        server.quit()  # 关闭连接
    except Exception as e:  # 如果 try 中的语句没有执行，则会执行下面的 ret=False
        ret = False
        logging.error(e)
    return ret


def run():
    schedule.every().day.at("08:50").do(do_sign)
    schedule.every().day.at("18:10").do(do_sign)
    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == '__main__':
    run()
