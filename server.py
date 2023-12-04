import json
from opcua import Server, ua
import time
import os
import random
from datetime import datetime
import pandas as pd
import queue
import threading
import copy

#The Goal is to create a Dataset to train a Neural Network.
#OPCUA Server Class. Reads an CSV file Containing All available Images to the EIBA Project.
#And loads them in a balanced way to into an image queue ready to be rated.
#Server handels multiple Clients that are connected to the Server.
#Once an Image got Score. The Image Path Score and a few more Informations to the Productgroup and so on are written down in an external csv_file.
#After The image is added to an Dictionary and its Used bool variable is ste to Truth. So it wound get loaded into the image queue again.      

class Service(object):

    def __init__(self, blocked, state):

        #initialize all important variables Nodes and classes.

        self.blocked = blocked
        self.state = state
        self.picture_service = self.PictureServer()
        self.max_queue_size = 30
        self.min_queue_size = 10
        self.score_data = pd.DataFrame(columns=['Date', 'User', 'image_name', 'oen', 'imageset', 'pg', 'score'])

        my_array = ua.Variant([], ua.VariantType.String)
        self.array_user = root_node.add_variable(f'ns={ns};s=username_array', "MyArray", val=my_array,
                                                 datatype=ua.NodeId(ua.ObjectIds.String))
        self.array_user.set_writable()
        self.remember_user = []
        self.array_length = 0
        background_thread_image_queue = threading.Thread(target=self.picture_service.add_images_to_queue, args=(
        self.picture_service.image_queue, self.min_queue_size, self.max_queue_size)
                                                         , daemon=True)
        background_thread_image_queue.start()
        background_thread_user_array = threading.Thread(target=self.define_user_nodes, daemon=True)
        background_thread_user_array.start()
        self.df = pd.read_csv(r'M:\DataSets\EIBA\01_Verlesedaten\02_MetaDaten\data_objects\prodGroupData_new.csv')
        self.index = 1
        self.username_array = server.get_node(f'ns={ns};s=username_array')
        self.username = root_node.add_variable(f'ns={ns};s=username', 'username', val=None)
        self.username.set_writable()
        pg_csv_file = root_node.add_object(f'ns={ns};s=obj_csv_file', bname=f'obj_csv_file')
        pg_csv_file.add_variable(f'ns={ns};s=csv_file', f'csv_file', val=None)
        self.get_csv_file = server.get_node(f'ns={ns};s=csv_file')


    def datachange_notification(self, node, val, data):
        
        #method on the OPC UA server implementation to handle data change notifications.
        #define username which activitate the datachange_notication method.

        username = server.get_node(f'ns={ns};s=username').get_value()

        if val == f'image_{username}' and self.blocked.get_value():
            
            #checks the val variable to identify if user wants an image, sends picture and all additional Information to the picture to the users Node. 
            
            self.blocked.set_value(False)
            img_path = self.picture_service.image_queue.get()
            dir_names = os.path.split(img_path)
            image_name = dir_names[1]
            dir_names = os.path.split(dir_names[0])
            image_set = dir_names[1]
            dir_names = os.path.split(dir_names[0])
            image_oen = dir_names[1]
            row = self.df['OEN'] == image_oen
            image_pg = self.df.loc[row, 'productGroup'].iloc[0]

            with open(img_path, "rb") as f:
                data = f.read()

            self.get_image_name = server.get_node(f'ns={ns};s=image_name_{username}')
            self.get_image_oen = server.get_node(f'ns={ns};s=image_oen_{username}')
            self.get_image_PG = server.get_node(f'ns={ns};s=image_PG_{username}')
            self.get_image_data = server.get_node(f'ns={ns};s=image_data_{username}')
            self.get_image_set = server.get_node(f'ns={ns};s=image_set_{username}')

            self.get_image_name.set_value(image_name)
            self.get_image_oen.set_value(image_oen)
            self.get_image_data.set_value(data)
            self.get_image_PG.set_value(image_pg)
            self.get_image_set.set_value(image_set)
            self.state.set_value(None)
            self.blocked.set_value(True)

        elif val == f'score_{username}' and self.blocked.get_value():
            
            #if user wants to save image and score to csv file
            #saves image, score and all additional information in csv file  

            self.blocked.set_value(False)

            get_image_name = server.get_node(f'ns={ns};s=image_name_{username}')
            get_image_oen = server.get_node(f'ns={ns};s=image_oen_{username}')
            get_image_PG = server.get_node(f'ns={ns};s=image_PG_{username}')
            get_image_score = server.get_node(f'ns={ns};s=image_score_{username}')
            get_image_set = server.get_node(f'ns={ns};s=image_set_{username}')

            name = get_image_name.get_value()
            oen = get_image_oen.get_value()
            pg = get_image_PG.get_value()
            set = get_image_set.get_value()
            score = get_image_score.get_value()

            row = [datetime.now().strftime('%d/%m/%Y-%H:%M%S'), username, name, oen, set,
                   pg, score]

            if all(row):
                self.score_data = pd.concat([self.score_data, pd.DataFrame([row], columns=self.score_data.columns)])
                self.score_data.to_csv(rated_pictures)
                self.index += 1
            self.state.set_value(None)
            self.blocked.set_value(True)

        elif val == f'csv_file' and self.blocked.get_value():
            
            #if user wants to have the csv file csv file node becomes updated and user can get all information to rated images.  
            
            self.blocked.set_value(False)
            with open(rated_pictures, "rb") as f:
                data = f.read()
            self.get_csv_file.set_value(data)
            self.state.set_value(None)
            self.blocked.set_value(True)

    def define_user_nodes(self):

        #if user connects to the sever, server creats all necessary Nodes that the user will need to rate images with his client script.

        while True:

            self.username_array = server.get_node(f'ns={ns};s=username_array')
            user_array = self.username_array.get_value()
            len_user_array = len(user_array)
            

            if len_user_array > self.array_length:
                user_object = root_node.add_object(f'ns={ns};s={user_array[len_user_array - 1]}',
                                                   bname=f'{user_array[len_user_array - 1]}')
                user_object.add_variable(f'ns={ns};s=image_name_{user_array[len_user_array - 1]}',
                                         f'image_name_{user_array[len_user_array - 1]}', val=None)
                user_object.add_variable(f'ns={ns};s=image_oen_{user_array[len_user_array - 1]}',
                                         f'image_oen_{user_array[len_user_array - 1]}', val=None)
                user_object.add_variable(f'ns={ns};s=image_PG_{user_array[len_user_array - 1]}',
                                         f'image_PG_{user_array[len_user_array - 1]}', val=None)
                user_object.add_variable(f'ns={ns};s=image_data_{user_array[len_user_array - 1]}',
                                         f'image_data_{user_array[len_user_array - 1]}', val=None)
                user_object.add_variable(f'ns={ns};s=image_view_{user_array[len_user_array - 1]}',
                                         f'image_view_{user_array[len_user_array - 1]}', val=None)
                user_object.add_variable(f'ns={ns};s=image_set_{user_array[len_user_array - 1]}',
                                         f'image_set_{user_array[len_user_array - 1]}', val=None)
                image_score = user_object.add_variable(f'ns={ns};s=image_score_{user_array[len_user_array - 1]}',
                                                       f'image_score_{user_array[len_user_array - 1]}', val=None)
                image_score.set_writable()
                user_object.add_variable(f'ns={ns};s=name_{user_array[len_user_array - 1]}',
                                         f'name_{user_array[len_user_array - 1]}', val=None)

                self.array_length += 1
                self.remember_user.append(user_array[len_user_array - 1])
            
            #if user disconnect from Server all user Nodes get deleted.

            if len_user_array < self.array_length:
                difference = set(self.remember_user).difference(set(user_array))
                for string in difference:
                    user_object_node = server.get_node(f'ns={ns};s={string}')
                    server.delete_nodes([user_object_node], recursive=True)
                    self.array_length -= 1
                    self.remember_user.remove(string)

            time.sleep(0.5)

    class PictureServer:

        #class to provide the pictures. Becomes initialized in the Server class.
        #Provides images in a balanced way. Image view and productgroups are provided in a balanced way.   

        def __init__(self):

            self.directory = r'M:\DataSets\EIBA\01_Verlesedaten\03_DataSet\Part Number'
            self.df = pd.read_csv(r'M:\DataSets\EIBA\01_Verlesedaten\02_MetaDaten\data_objects\prodGroupData_new.csv')
            self.load_data()
            self.load_data_pg()
            self.image_queue = queue.Queue()
            self.list_all_oens = self.get_folder_names_without_spaces()
            self.write_lock = threading.Lock()
            self.max_attemps = 10
            self.step = 0
            self.pg = self.df['productGroup'].unique()

        def save_data(self,dictionary):
            #function to save the dictionary with all used pictures.
            with open('used_items.json', 'w') as f:
                json.dump(dictionary, f)
                

        def load_data(self):
            #Function to load dictionary with all used pictures
            try:
                with open('used_items.json', 'r') as f:
                    self.used_items = json.load(f)
            except:
                self.used_items = {}

        def load_data_pg(self):
            #load dictionary with all used pgs
            try:
                with open('pg.json', 'r') as f:
                    self.pg_dict = json.load(f)
            except:
                self.pg_dict = {pg: 0 for pg in self.df['productGroup'].unique()}

        def save_data_pg(self,dictionary):
            #save dictionary with all used pgs
            with open('pg.json', 'w') as f:
                json.dump(dictionary, f)

        def get_folder_names_without_spaces(self):
            #helper function to remove all spaces within an folder name
            folder_names = []
            with os.scandir(self.directory) as entries:
                for entry in entries:
                    if entry.is_dir() and " " not in entry.name:
                        folder_names.append(entry.name)
            return folder_names

        def add_image(self, pg, oen, set_name, image_name):
            
            #Add pgs, oens, sets and images to the dictionary if there are not already there. 
            #initialize a used or faulty bool for pg, oen, set and image to set them to already used or not available.
            self.write_lock.acquire()
            if not isinstance(set_name, str):
                try:
                    set_name = set_name.name
                except:
                    print('Error getting str of set name')
                    self.write_lock.release
                    return
            if pg not in self.used_items:
                self.used_items[pg] = {
                    'used_or_faulty': False,
                    'use_cnt' : 0,
                    'oens': {}
                }

            if oen not in self.used_items[pg]['oens']:
                self.used_items[pg]['oens'][oen] = {
                    'used_or_faulty': False,
                    'sets': {}
                }

            if set_name not in self.used_items[pg]['oens'][oen]['sets']:
                self.used_items[pg]['oens'][oen]['sets'][set_name] = {
                    'used_or_faulty': False,
                    'view_cnt': {'Top': 0, 'Left': 0, 'Right': 0},
                    'pictures': []
                }
            
            if 'Top' in image_name:  
                if self.used_items[pg]['oens'][oen]['sets'][set_name]['view_cnt']['Top'] >= 1:
                    self.write_lock.release()
                    return
                self.used_items[pg]['oens'][oen]['sets'][set_name]['view_cnt']['Top'] += 1

            elif 'Right' in image_name:
                if self.used_items[pg]['oens'][oen]['sets'][set_name]['view_cnt']['Right'] >= 1:
                    self.write_lock.release()
                    return
                self.used_items[pg]['oens'][oen]['sets'][set_name]['view_cnt']['Right'] += 1

            elif 'Left' in image_name:
                if self.used_items[pg]['oens'][oen]['sets'][set_name]['view_cnt']['Left'] >= 1:
                    self.write_lock.release()
                    return
                self.used_items[pg]['oens'][oen]['sets'][set_name]['view_cnt']['Left'] += 1

            else:
                self.write_lock.release()
                return None
            
            self.used_items[pg]['oens'][oen]['sets'][set_name]['pictures'].append(image_name)

            if len(self.used_items[pg]['oens'][oen]['sets'][set_name]['pictures']) >= 3:

                self.used_items[pg]['oens'][oen]['sets'][set_name]['used_or_faulty'] = True

                if len([x for x in self.used_items[pg]['oens'][oen]['sets'] if self.used_items[pg]['oens'][oen]['sets'][x]['used_or_faulty']]) >= len([x for x in os.listdir(os.path.join(self.directory, oen)) if os.path.isdir(os.path.join(self.directory, oen, x))]):

                    self.used_items[pg]['oens'][oen]['used_or_faulty'] = True

                    if len([x for x in self.used_items[pg]['oens'] if self.used_items[pg]['oens'][x]['used_or_faulty']]) >= len([oen for oen in self.df.loc[self.df['productGroup'] == pg, 'OEN'].unique() if oen in self.list_all_oens]):
                        self.used_items[pg]['used_or_faulty'] = True

            self.save_data(copy.deepcopy(self.used_items))
            self.write_lock.release()

        def get_image(self, max_attemps, step):

            #getting images from EiBA directory. In a balanced way. pictures gets povided so all imageviews and all prouductgroups are equally presented to the client.  
            #Using the dictionary by checking the used or faulty 'bool' to make sure images are not presented to client more than once.
            search_pg_list = {} 
            while step <= max_attemps:
                for pg in self.pg_dict:
                    if not self.used_items.get(pg, {'used_or_faulty': False}).get('used_or_faulty', False):
                        search_pg_list[pg] = self.pg_dict[pg]  
                pg_list = list(search_pg_list.keys())
                pg = random.choice(sorted(pg_list, key=lambda x: search_pg_list[x])[0:3])
                search_oen_list = [oen for oen in self.df.loc[self.df['productGroup'] == pg, 'OEN'].unique() if oen in self.list_all_oens and not self.used_items.get(pg, {}).get('oens', {}).get(oen, {}).get('used_or_faulty', False)]
                    
                if search_oen_list and len(search_oen_list) > 0:
                    oen = random.choice(search_oen_list)
                    search_set_list = [set_name.name for set_name in os.scandir(os.path.join(directory, oen)) if set_name.is_dir() and not self.used_items.get(pg, {}).get('oens', {}).get(oen, {}).get('sets', {}).get(set_name.name, {}).get('used_or_faulty', {})]
                    
                    if len(search_set_list) == 1:
                        set_name = search_set_list[0]
                        image_list = []
                        for image in os.listdir(os.path.join(directory, oen, set_name)):
                            if 'Part' in image and not 'mask' in image and image.endswith('.jpg'):
                                image_list.append(os.path.join(directory, oen, set_name, image))
                                if image_list:
                                    if pg not in self.used_items:
                                        self.used_items[pg] = {
                                            'used_or_faulty': False,
                                            'use_cnt' : 0,
                                            'oens': {}
                                        }

                                    if oen not in self.used_items[pg]['oens']:
                                        self.used_items[pg]['oens'][oen] = {
                                            'used_or_faulty': False,
                                            'sets': {}
                                        }

                                    if set_name not in self.used_items[pg]['oens'][oen]['sets']:
                                        self.used_items[pg]['oens'][oen]['sets'][set_name] = {
                                            'used_or_faulty': False,
                                            'view_cnt': {'Top': 0, 'Left': 0, 'Right': 0},
                                            'pictures': []
                                        }
                                    
                                    self.used_items[pg]['oens'][oen]['used_or_faulty'] = True
                                    return random.choice(image_list)

                    
                    if search_set_list and len(search_set_list) > 1:
                        set_name = random.choice(search_set_list)
                        image_search_list = []
                        for image in os.listdir(os.path.join(directory, oen, set_name)):
                            if 'Part' in image and not 'mask' in image and image.endswith('.jpg'): 
                                if not os.path.join(directory, oen, set_name, image) in self.used_items.get(pg, {}).get('oens', {}).get(oen, {}).get('sets', {}).get(set_name, {}).get('pictures', {}):
                                    image_search_list.append(os.path.join(directory, oen, set_name, image))
                            
                        if image_search_list and len(image_search_list) > 0:
                            image_path = random.choice(image_search_list)
                            if image_path and image_path not in list(self.image_queue.queue):
                                step = 0
                                return image_path
                        else:
                            self.used_items[pg]['oens'][oen]['used_or_faulty'] = True
                
                else:
                
                    if pg not in self.used_items:
                        self.used_items[pg] = {
                            'used_or_faulty': True,
                            'used_cnt': 0,
                            'oens': {}
                        }
                    else:
                        self.used_items[pg]['used_or_faulty'] = True

                step += 1

        def add_images_to_queue(self, image_queue, min_queue_size, max_queue_size):
            
            #Adding the images to the image queue.  
            
            while True:

                if image_queue.qsize() < min_queue_size:
                    while image_queue.qsize() < max_queue_size:
                        image_path = self.get_image(self.max_attemps, self.step)
                        if image_path:
                            self.image_queue.put(image_path)
                            dir_names = os.path.split(image_path)
                            image_name = dir_names[1]
                            dir_names = os.path.split(dir_names[0])
                            image_set = dir_names[1]
                            dir_names = os.path.split(dir_names[0])
                            image_oen = dir_names[1]
                            row = self.df['OEN'] == image_oen
                            image_pg = self.df.loc[row, 'productGroup'].iloc[0]
                            self.pg_dict[image_pg]+=1
                            self.add_image(pg=image_pg, oen=image_oen, set_name=image_set, image_name=image_name)
                        pass
                time.sleep(3)


if __name__ == '__main__':
    # Pfad zur CSV f�r Rated Pictures
    rated_pictures = r'M:\DataSets\EIBA\01_Verlesedaten\02_MetaDaten\data_objects\RatedPictures.csv'
    # Pfad zu productgroup excel
    product_group_excel = r'M:\DataSets\EIBA\01_Verlesedaten\02_MetaDaten\data_objects\prodGroupData_new.csv'
    # Pfad zur Bild directory
    directory = r'M:\DataSets\EIBA\01_Verlesedaten\03_DataSet\Part Number'

    start_part = 'Part_'
    server = Server()
    #ip = '172.17.21.69'
    # port = '3000'
    ip = '127.0.0.1'
    port = '9000'

    con = f'opc.tcp://{ip}:{port}'
    print(f'connection: {con}')
    server.set_endpoint(con)
    ns = server.register_namespace('Image_Rating')
    root_node = server.get_root_node()
    buffer = 50
    client_message = root_node.add_object(f'ns={ns};s=client_message', bname='client_message')
    state = client_message.add_variable(f'ns={ns};s=state', 'Got new Image', val=None)
    state.set_writable()
    blocked = client_message.add_variable(f'ns={ns};s=blocked', 'Got new Image', True)

    server.start()
    print("Start Server")
    service_object = Service(blocked, state)
    sub = server.create_subscription(buffer, service_object)
    handle = sub.subscribe_data_change(state)

    print('############################')

    while True:
        time.sleep(3)
