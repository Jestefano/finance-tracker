import os

from dotenv import load_dotenv
import telebot
import boto3

from source.auxiliar import preprocessing_info, file_names, generate_json, save_info,extract_today_info,\
    response_to_df, message_check_in, validate_previous_add, change_state, validate_state_time
from source.responses import instructions_error, instructions_start

load_dotenv()

# # Variables
BOT_TOKEN = os.getenv('BOT_TOKEN')
BUCKET_NAME = os.getenv('BUCKET_NAME')
DB_NAME = os.getenv('DB_NAME')
TABLE_NAME = os.getenv('TABLE_NAME')
TELEGRAM_IDS = os.getenv('TELEGRAM_IDS')

# Telegram
bot = telebot.TeleBot(BOT_TOKEN)

# boto3
s3 = boto3.resource('s3')
s3_client = boto3.client('s3')
client = boto3.client('athena')
    
def send_welcome_message(message):
    bot.send_message(message['chat']['id'], instructions_start)

def add_registries_message(text, message):
    if text == '/add':
        bot.send_message(message['chat']['id'], "Add your registry with (ammount,category,card,detail,relation)")
    if text == '/addother':
        bot.send_message(message['chat']['id'], "Add your registry with (YYYY-mm-dd,ammount,category,card,detail,relation)")
    
def add_registries_func(message):
    key = file_names(s3, BUCKET_NAME, message['text'])
    data = generate_json(message['text'])
    preprocessing_info(data)
    save_info(s3, client, bot, DB_NAME, BUCKET_NAME, TABLE_NAME, key, data, message['chat']['id'])

def see_last_x_registries(message):
    results = extract_today_info(s3_client, client, BUCKET_NAME, DB_NAME, TABLE_NAME)
    df = response_to_df(results)
    # Falta beautify 
    bot.reply_to(message,str(df))

def remaining_month(message):
    pass

def echo_all(message):
    bot.reply_to(message, instructions_error)

def lambda_handler(event, context):
    message, text = message_check_in(event)
    
    if text == '/start' or text == '/hello':
        change_state(s3, BUCKET_NAME, 'None')
        send_welcome_message(message)
    elif text == '/see':
        change_state(s3, BUCKET_NAME, 'None')
        see_last_x_registries(message)
    elif text  == '/add' or text == '/addother':
        change_state(s3, BUCKET_NAME, 'Add')
        add_registries_message(text,message)    
    elif validate_previous_add():
        change_state(s3, BUCKET_NAME, 'Add')
        add_registries_func(message)
    else:
        echo_all(message)
    
    validate_state_time(s3, BUCKET_NAME)
        
    return {'status':200}