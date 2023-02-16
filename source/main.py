import datetime
import os
import time
from functools import wraps

from dotenv import load_dotenv
import telebot
import boto3

from auxiliar import preprocessing_info, file_names, generate_json, save_info,extract_today_info,response_to_df
from responses import instructions_error, instructions_start

load_dotenv()

# Variables
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

def restricted(func):
    """Restrict usage of func to allowed users only and replies if necessary"""
    @wraps(func)
    def wrapped(bot, update, *args, **kwargs):
        user_id = update.effective_user.id
        if user_id not in TELEGRAM_IDS:
            print("WARNING: Unauthorized access denied for {}.".format(user_id))
            update.message.reply_text('User disallowed.')
            return  # quit function
        return func(bot, update, *args, **kwargs)
    return wrapped

@restricted
@bot.message_handler(commands=['start', 'hello'])
def send_welcome(message):
    bot.reply_to(message, instructions_start)

@restricted
@bot.message_handler(commands=['add']) 
def add_registries_today(message):
    text = "Add your registry with (ammount,category,card,detail,relation)"
    sent_msg = bot.send_message(message.chat.id, text)
    bot.register_next_step_handler(sent_msg, add_registries_func)

@restricted
@bot.message_handler(commands=['addpast'])
def add_registries_other_day(message):
    text = "Add your registry with (YYYY-mm-dd,ammount,category,card,detail,relation)"
    sent_msg = bot.send_message(message.chat.id, text)
    bot.register_next_step_handler(sent_msg, add_registries_func)
    
def add_registries_func(message):
    key = file_names(s3,BUCKET_NAME,message.text)
    data = generate_json(message.text)
    preprocessing_info(data)
    save_info(s3, client, bot, DB_NAME, BUCKET_NAME, TABLE_NAME, key, data, message.chat.id)

@restricted
@bot.message_handler(commands=['see'])
def see_last_x_registries(message):
    results = extract_today_info(s3_client, client, BUCKET_NAME, DB_NAME, TABLE_NAME)
    df = response_to_df(results)
    # Falta beautify 
    bot.reply_to(message,str(df))

@restricted
@bot.message_handler(commands=['rem'])
def remaining_month(message):
    pass

@restricted
@bot.message_handler(func=lambda msg: True)
def echo_all(message):
    bot.reply_to(message, instructions_error)
    
bot.infinity_polling()