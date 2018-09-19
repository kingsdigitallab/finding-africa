#!/usr/bin/env python

import configparser
import email
import imaplib
import logging
import logging.config
import os
import re
import shutil
import smtplib
from datetime import datetime
from email.message import EmailMessage

import yaml

import pandas as pd
from lxml import etree

imaplib.IMAP4.debug = imaplib.IMAP4_SSL.debug = 1


def configure_logging(default_path='logging.yml', default_level=logging.INFO):
    path = default_path

    if os.path.exists(path):
        with open(path, 'rt') as f:
            config = yaml.safe_load(f.read())
            logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)


def prepare(config):
    if not os.path.exists(config.get('DEFAULT', 'error')):
        os.makedirs(config.get('DEFAULT', 'error'))

    if not os.path.exists(config.get('DEFAULT', 'output')):
        os.makedirs(config.get('DEFAULT', 'output'))

    if not os.path.exists(config.get('DEFAULT', 'sandbox')):
        os.makedirs(config.get('DEFAULT', 'sandbox'))

    if not os.path.exists(config.get('DEFAULT', 'success')):
        os.makedirs(config.get('DEFAULT', 'success'))


def get_data_from_mailbox(config):
    logger = logging.getLogger()

    address = config.get('mailbox', 'address')
    username = config.get('mailbox', 'username')
    password = config.get('mailbox', 'password')

    if not address or not username or not password:
        logger.error('Invalid mailbox confiration, check the config.')
        return None

    logger.debug('Connecting via ssl IMAP...')
    mailbox = imaplib.IMAP4_SSL(address, 993)
    logger.debug('Conected!!')

    logger.debug('Trying to login via ssl')
    mailbox.login(username, password)
    logger.debug('logged in!!')

    logger.debug('Selecting INBOX')
    mailbox.select('Inbox')
    logger.debug('Getting unread messages...')
    typ, data = mailbox.search(None, '(UNSEEN)')

    counter = 0
    attachments = {}

    for num in data[0].split():
        typ, data = mailbox.fetch(num, 'BODY.PEEK[]')

        text = data[0][1]
        msg = email.message_from_bytes(text)

        sender = re.sub('<|>', '', msg['Return-Path'])
        logger.debug('Message from: {}'.format(sender))

        # skips unknown addresses
        if not is_email_address_known(config, sender):
            logger.info('Unknown sender: {}'.format(sender))
            continue

        for part in msg.walk():
            if part.get_content_maintype() == 'multipart':
                continue

            if part.get('Content-Disposition') is None:
                continue

            data = part.get_payload(decode=True)
            if not data:
                logger.debug('{}: No attachments found...'.format(sender))
                continue

            seq, filename = save_attachment(config, sender, data)
            attachments[filename] = sender
            logger.info(
                '{}: Downloaded attachment {}'.format(sender, filename))

            update_sequence(config, 'config.ini', sender, seq)

            counter += 1

        logger.info('Downloaded {} attachment(s)'.format(counter))

        # marks the message as read
        mailbox.fetch(num, 'BODY[]')

    mailbox.close()
    mailbox.logout()

    return attachments


def is_email_address_known(config, address):
    return config.has_section(address)


def save_attachment(config, address, attachment):
    sandbox = config.get('DEFAULT', 'sandbox')

    code = config.get(address, 'code')

    seq = 1
    if config.has_option(address, 'sequence'):
        seq = config.getint(address, 'sequence') + 1

    filename = os.path.join(sandbox, '{}_{}.xlsx'.format(code, seq))

    with open(filename, 'wb') as f:
        f.write(attachment)
        f.close()

    return seq, filename


def update_sequence(config, filename, address, seq):
    config.set(address, 'sequence', str(seq))

    with open(filename, 'w') as f:
        config.write(f)


def process_attachments(config, attachments):
    logger = logging.getLogger()

    if not attachments:
        logger.info('No attachments found')
        return

    logger.debug('Processing attachments: {}'.format(attachments))

    error_path = config.get('DEFAULT', 'error')
    in_path = config.get('DEFAULT', 'sandbox')
    out_path = config.get('DEFAULT', 'output')
    success_path = config.get('DEFAULT', 'success')

    for root, dirs, files in os.walk(in_path):
        for name in files:
            filename, ext = os.path.splitext(name)

            if ext == '.xlsx':
                try:
                    filepath = os.path.join(root, name)
                    logger.info('{}: processing attachment'.format(filepath))

                    if filepath not in attachments:
                        logger.warning(
                            ('Skipping files that are not associated with an '
                             'email address: {}').format(filepath)
                        )
                        continue

                    collection = clean_collection(filepath)
                    email = attachments[filepath]

                    missing_fields = get_missing_fields(collection)
                    if missing_fields:
                        logger.warning('{}: has missing fields {}'.format(
                            filepath, missing_fields))
                        shutil.move(filepath, os.path.join(error_path, name))

                        logger.info('Sending failure report to: {}'.format(
                            email))
                        send_failure_report(config, email, missing_fields)
                        continue

                    filename = os.path.splitext(name)[0]

                    collection_filename = os.path.join(
                        out_path, '{}.xml'.format(filename))
                    process_collection(collection, collection_filename)
                    process_terms(filepath, out_path)

                    success_filepath = os.path.join(success_path, name)
                    shutil.move(filepath, success_filepath)

                    send_success_report(config, email, collection_filename)
                except Exception as e:
                    logger.error('{}: failed to process'.format(filepath))
                    logger.error(e.args)
                    shutil.move(filepath, os.path.join(error_path, name))

                logger.info('{}: processed attachment'.format(filepath))


def clean_collection(filename):
    collection = pd.read_excel(filename, sheet_name='collection')
    collection.drop('ARCHIVES AFRICA: COLLECTION DATA', axis=1).drop(0)

    collection = collection.transpose()
    new_header = collection.iloc[0].str.strip()
    collection = collection[1:]
    collection.columns = new_header

    collection.drop(['* Required'], axis=1, inplace=True)

    return collection


def get_missing_fields(collection):
    missing_fields = []

    for c in collection.columns:
        # required fields
        if '*' in c:
            data = collection[c][1]
            if pd.isna(data) or pd.isnull(data):
                missing_fields.append(c)

    return missing_fields


def send_failure_report(config, email, missing_fields):
    lang = config.get(email, 'language')
    if not lang:
        lang = 'en'

    message = 'Missing fields'
    with open(config.get('reports', 'failure_{}'.format(lang))) as f:
        message = f.read()
        f.close()

    send_email(config, email, 'Missing fields', '{}\n{}'.format(
        message, missing_fields))


def send_email(config, to, subject, message):
    address = config.get('mailbox', 'address')
    username = config.get('mailbox', 'username')
    password = config.get('mailbox', 'password')

    if not address or not username or not password:
        logger.error('Invalid mailbox configuration, check the config.')
        return

    msg = EmailMessage()
    msg.set_content(message)

    msg['Subject'] = subject
    msg['From'] = username
    msg['To'] = to

    s = smtplib.SMTP(address)
    s.login(username, password)
    s.send_message(msg)
    s.quit()


def process_collection(collection, filename):
    xml = collection_to_xml(collection)
    save_xml(xml, filename)


def collection_to_xml(collection):
    xml = etree.Element('collection')

    for c in collection.columns:
        data = collection[c][1]

        if not pd.isna(data) and not pd.isnull(data):
            name = collection[c][0]
            name = re.sub(r'\W', '', name)
            el = etree.SubElement(xml, name)

            if isinstance(data, str):
                paras = data.split('\n')

                for para in paras:
                    para = para.strip()
                    if para:
                        p = etree.SubElement(el, 'p')
                        if '<' in para or '>' in para:
                            p.text = etree.CDATA(para)
                        else:
                            p.text = para
            elif isinstance(data, datetime):
                data = data.date().isoformat()
                el.text = data
            else:
                el.text = data

    return etree.ElementTree(xml)


def save_xml(xml, name):
    logger.debug('save_xml: {}'.format(name))
    xml.write(name, encoding='utf-8', method='xml', pretty_print=True)


def process_terms(xlsx, path):
    excel = pd.ExcelFile(xlsx)

    for sn in excel.sheet_names[1:]:
        df = pd.read_excel(excel, sn)

        el_name = df.columns[0].split(':')[0]
        el_name = el_name.strip().lower().replace(' ', '_')

        xml = terms_to_xml(df, el_name)

        filename = os.path.basename(xlsx).split('.')[0]
        save_xml(
            xml, os.path.join(path, '{}_{}.xml'.format(filename, el_name)))


def terms_to_xml(terms, root):
    xml = etree.Element(root)

    for idx, row in terms.iterrows():
        if idx > 0:
            p = etree.SubElement(xml, 'p')

            for pos, term in enumerate(row):
                if not pd.isna(term) and not pd.isnull(term):
                    name = terms.iloc[0][pos].split(':')[0]
                    name = name.lower().replace('>', '')
                    name = name.strip().replace(' ', '_')
                    name = re.sub(r'\W', '', name)

                    el = etree.SubElement(p, name)
                    el.text = term

    return etree.ElementTree(xml)


def send_success_report(config, email, filepath):
    lang = config.get(email, 'language')
    if not lang:
        lang = 'en'

    message = 'Thank you for your email'
    with open(config.get('reports', 'success_{}'.format(lang))) as f:
        message = f.read()
        f.close()
    send_email(config, email, 'Thank you for your email', message)

    to = config.get('reports', 'email')
    if not to:
        logger.error('Missing email address to send success reports')

    send_email(config, to, 'New files added', 'From {}, {}'.format(
        email, filepath))


if __name__ == '__main__':
    # the current logging configuration is logging to the console, change to
    # log to a file with rotation before deploying
    configure_logging()
    logger = logging.getLogger()
    logger.info('Processor: start')

    logger.info('Processor: reading config')
    config = configparser.ConfigParser()
    config.read('config.ini')

    logger.info('Processor: preparing directories from the config')
    prepare(config)

    logger.info('Processor: getting attachments')
    attachments = get_data_from_mailbox(config)

    logger.info('Processor: processing attachments')
    process_attachments(config, attachments)
    logger.info('Processor: end')
