import requests
import pandas as pd

# api字典
config_dict = {
    "终端升级清单": {
        "url": "http://sjgxpt.telecomjs.com:8090/dataway/api/zd_reporter/common/wod/dzxp/ZDSJQD",
        "headers": {
            'appKey': 'bb7b0ad236c26563',
            'appSecret': '1b1f3974bb7b0ad236c26563c783f989'
        }
    }
}

# 获取数据
def get_api_data(api_name):
    # 获取api信息
    config = config_dict[api_name]
    url = config['url']
    headers = config['headers']

    try:
        # 发送POST请求
        response = requests.post(url, headers=headers)
        # 检查响应状态码
        response.raise_for_status()
        # 解析JSON响应
        response_data = response.json()
        if response_data.get('success') and 'value' in response_data:
            # 将JSON数据转换为DataFrame
            df = pd.DataFrame(response_data['value'])
            print("成功转换为DataFrame")
            return df
        else:
            print("返回数据格式不正确，无法转换为DataFrame")
            return pd.DataFrame()
            
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        print(f"Response content: {response.content if 'response' in locals() else 'N/A'}")
        return pd.DataFrame()
    
    except Exception as e:
        print(f"数据处理失败: {e}")
        return pd.DataFrame()

# 发送邮件
def send_email(receiver_emails, cc_emails, subject, body_content, attachment_path,sender_email,sender_password):
    """
        receiver_emails (str): 收件人邮箱，逗号分隔
        cc_emails (str): 抄送人邮箱，逗号分隔
        subject (str): 邮件主题
        body_content (str): 邮件正文（HTML格式）
        attachment_path (str): 附件路径
        sender_email (str): 发件人邮箱
        sender_password (str): 发件人密码
    """
    import os
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    from email.mime.application import MIMEApplication
    
    if not receiver_emails:
        print("收件人邮箱为空，跳过发送")
    
    # 处理邮箱列表
    receiver_email_list = [address.strip("'") for address in receiver_emails.split(",")]
    cc_email_list = [address.strip("'") for address in cc_emails.split(",")] if cc_emails else []
    
    msg = MIMEMultipart()
    msg['Subject'] = subject
    msg['From'] = sender_email
    msg['To'] = ','.join(receiver_email_list)
    msg['Cc'] = ','.join(cc_email_list)
    
    # 添加邮件正文
    body = MIMEText(body_content, 'html')
    msg.attach(body)
    
    # 添加附件
    if attachment_path:
        try:
            with open(attachment_path, 'rb') as f:
                attachment = MIMEApplication(f.read(), _subtype='xlsx')
                filename = os.path.basename(attachment_path)
                attachment.add_header('Content-Disposition', 'attachment', filename=filename)
                msg.attach(attachment)
        except Exception as e:
            print(f"添加附件失败: {e}")
    
    # 发送邮件
    try:
        with smtplib.SMTP_SSL('smtp.chinatelecom.cn', 465) as smtp:
            smtp.login(sender_email, sender_password)
            smtp.sendmail(sender_email, receiver_email_list + cc_email_list, msg.as_string())
        print("邮件发送成功")
    except Exception as e:
        print(f"邮件发送失败: {e}")


if __name__ == '__main__':

    # 获取数据    
    api_name = "终端升级清单"
    df = get_api_data(api_name)
    
    # 打印数据
    print(df.head(1))