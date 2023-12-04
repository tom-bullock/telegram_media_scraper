from telethon import TelegramClient
from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument
import pandas as pd
import datetime
import os

'''
This script is designed to scrape text and media from public telegram channels.
The script requires you to obtain a telegram account and api key. This can be done
with zero validation for free online at: 

To use the script enter your telegram api id and api hash id, followed by either a
single or list of telegram channel ids. Channel ids can be found here:

You can also provide a path to a txt file containing a list of comma separated telegram
channel ids.

You will also be prompted to enter output paths for a directory to save the media
from the channel and the csv containing each post's text and metadata.
'''

#### creates directory if needed
def create_directory_if_not_exists(directory_path):
    # Check if the directory exists
    if not os.path.exists(directory_path):
        # Create the directory if it doesn't exist
        os.makedirs(directory_path)

#### gets newest item in directory and rename (for downloading images and video)
def rename_most_recently_added_file_with_id(directory, id_number):
    # Get a list of all items in the directory
    all_items = os.listdir(directory)

    # Full paths of all items
    full_paths = [os.path.join(directory, item) for item in all_items]

    # Filter only files (not directories)
    files = [f for f in full_paths if os.path.isfile(f)]

    # Get the most recently added file based on modification time
    most_recent_file = max(files, key=os.path.getmtime, default=None)

    # Extract the file name without the path
    most_recent_file_name = os.path.basename(most_recent_file) if most_recent_file else None

    # Add ID number to the file name and construct the new file name
    if most_recent_file_name:
        file_name_parts = os.path.splitext(most_recent_file_name)
        new_file_name = f"{file_name_parts[0]}_{id_number}{file_name_parts[1]}"

        # Construct the new full path
        new_full_path = os.path.join(directory, new_file_name)

        # Rename the file on the disk
        os.rename(most_recent_file, new_full_path)

        return new_file_name

async def message_data(message, cl, client, output_path_media):
    output_path_media = os.path.join(output_path_media, cl)
    create_directory_if_not_exists(output_path_media)

    # Lists for output DF
    d1, d2, d3, d4, d5, d6 = [], [], [], [], [], []

    # Get message text
    message_text = str(message.message)

    # Append message ID and message content
    message_id = message.id
    d1.append(message_id)
    d2.append(message_text)

    # Append attached media type to message
    if isinstance(message.media, (MessageMediaPhoto, MessageMediaDocument)):
        if isinstance(message.media, MessageMediaPhoto):
            d3.append('photo')
            await client.download_media(message.media, file= output_path_media)
            d6.append(rename_most_recently_added_file_with_id(output_path_media, message_id))
        elif isinstance(message.media, MessageMediaDocument):
            d3.append('video/mp4')
            await client.download_media(message.media.document, file= output_path_media)
            d6.append(rename_most_recently_added_file_with_id(output_path_media, message_id))
        else:
            d3.append('unknown')
            d6.append('unknown')
    else:
        d3.append('unknown')
        d6.append('unknown')


    # Format publish date/time and append to list
    date = str(message.date)
    d4.append(date)

    # Format and append source
    source = f't.me/{cl}/{message_id}'
    d5.append(source)

    # Output DataFrame
    output = pd.DataFrame({
        'message_id': d1,
        'message_content': d2,
        'message_media_type': d3,
        'message_published_at_date': d4,
        'message_source': d5,
        'message_media_file_name':d6
    })

    return output

#### scrapes posts from channels
def channel_scraper(channel_links, output_path, output_path_media, client, date_limit):

    outputs = []

    #### IF DATE LIMIT IS FALSE, SCRIPT WILL SCRAPE ALL POSTS
    if date_limit == False:
        #### FOR LOOP FOR INPUT OF MULTIPLE CHANNEL IDs
        if type(channel_links) is list:
            for cl in channel_links:
                async def main():
                    channel = await client.get_entity(cl)

                    async for message in client.iter_messages(channel, reverse=True):
                        output = await message_data(message, cl, client, output_path_media)
                        outputs.append(output)

                with client:
                    client.loop.run_until_complete(main())
        else:
            #### SINGLE CHANNEL ID VERSION
            async def main():
                channel = await client.get_entity(channel_links)

                async for message in client.iter_messages(channel, reverse=True):
                    output = await message_data(message, channel_links, client, output_path_media)
                    outputs.append(output)
            with client:
                client.loop.run_until_complete(main())

    else:

        #### FUNCTION TO RUN COLLECT WITH DATE LIMIT
    
        #### FOR LOOP FOR INPUT OF MULTIPLE CHANNEL IDs
        if type(channel_links) is list:
            for cl in channel_links:
                async def main():
                    channel = await client.get_entity(cl)

                    async for message in client.iter_messages(channel, reverse=True, offset_date= date_limit):
                        output = await message_data(message, cl, client, output_path_media)
                        outputs.append(output)
                with client:
                    client.loop.run_until_complete(main())
        else:
            #### SINGLE CHANNEL ID VERSION
            async def main():
                channel = await client.get_entity(channel_links)

                async for message in client.iter_messages(channel, reverse=True, offset_date= date_limit):
                    output = await message_data(message, channel_links, client, output_path_media)
                    outputs.append(output)
            with client:
                client.loop.run_until_complete(main())

    df = pd.concat(outputs, ignore_index= False)

    df.to_csv(os.path.join(output_path, f'telegram_scrape.csv'), index= False)

    print(f'scraped {len(df)} posts')

#### functions to get channel ids
def get_single_id():
    user_input = input('enter a single channel id: ')
    return [user_input.strip()]

def get_list_of_ids():
    user_input = input('enter a list of channel ids separated by commas: ')
    id_list = [id.strip() for id in user_input.split(',')]
    return id_list

def read_ids_from_file(file_path):
    try:
        with open(file_path, 'r') as file:
            content = file.read()
            id_list = [id.strip() for id in content.split(',')]
            return id_list
    except FileNotFoundError:
        print(f'error: file not found at path {file_path}')
        return []

def select_channel_id_type():
    print('choose an option:')
    print('1. enter a single channel id')
    print('2. enter a list of comma separeated channel ids')
    print('3. provide a path to a text file containing comma separated channel ids')

    choice = input('enter your choice (1, 2, or 3): ')

    if choice == '1':
        ids = get_single_id()
    elif choice == '2':
        ids = get_list_of_ids()
    elif choice == '3':
        file_path = input('Enter the path to the text file: ')
        ids = read_ids_from_file(file_path)
    else:
        print('invalid choice. please enter 1, 2, or 3.')
        return

    return ids


#### check format on date_limit 
def enter_date_limit():
    while True:
        date_limit = input('enter date limit (YYYY-MM-DD) for scrape (leave blank to scrape entire channel): ')
        if len(date_limit) == 0:
            date_limit = False
            break
        else:
            try:
                date_limit = datetime.datetime.strptime(date_limit, "%Y-%m-%d")
                break
            except ValueError:
                print("Error: Invalid date format. Try again.")
    
    return date_limit

#### protocol to run the script in terminal

def main():
    #### input telegram credentials and launch telegram client
    api_id = input('enter your telegram api id: ')
    api_hash = input('enter telegram api hash id: ')
    client = TelegramClient('anon', api_id, api_hash)

    #### input channel ids and date_limit for collect
    channel_links = select_channel_id_type()
    date_limit = enter_date_limit()

    #### set output paths 
    output_path_csv = input('enter directory path for output csv: ') 
    output_path_media = input('enter directory path for media: ')

    channel_scraper(channel_links, output_path_csv, output_path_media, client, date_limit)

while True:
    if __name__ == "__main__":
        main()
