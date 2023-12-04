'''
Scripttitel: OPCUA image rating Client
Hersteller: Benjamin Dierk
Datum: 15.04.2023
Beschreibung: A client script that connects to an opcua server with build in gui to rate images.  
'''

from opcua import Client ,ua
import time
import tkinter as tk
from PIL import Image, ImageTk
import io
from concurrent.futures import ThreadPoolExecutor, TimeoutError
import csv


class GUI:
    
    # Graphical user Interface class to rate pictures

    def __init__(self,user_name):
        
        #initialize username, the opcua client class, the Window amd widgets of the GUI
        #Also paramters to define window size image size and so on.
        
        self.user_name = user_name
        #self.server_class = self.ServiceClient(self.user_name)
        self.root = tk.Tk()
        screen_width = self.root.winfo_screenwidth()
        screen_height = self.root.winfo_screenheight()
        window_width = int(screen_width * 0.9)
        window_height = int(screen_height * 0.8)
        image_ratio = 1280 / 720
        self.image_width = int(screen_width * 0.6)
        self.image_height = int(self.image_width / image_ratio)
        
        self.root.title("Image rating interface")
        self.root.geometry(f"{window_width}x{window_height}")
        
        widget_width = int(window_width * 0.7)
        widget_height = int(window_height * 1)
        button_width = int(screen_width * 0.005)
        button_height = int(screen_height * 0.005)
        
        self.root.protocol("WM_DELETE_WINDOW", self.exit_program)
    
        start_image = r'Instructions.jpg'
        self.image = Image.open(start_image)
        self.image = self.image.resize((self.image_width, self.image_height), Image.Resampling.LANCZOS)
        self.photo = ImageTk.PhotoImage(self.image)
        self.label = tk.Label(self.root, image=self.photo,width=widget_width, height=widget_height)
        self.label.grid(row=0, column=0,rowspan=7, columnspan=7)
        
        self.button_exit = tk.Button(self.root, text="Exit",command=self.exit_program,width= button_width, height=button_height)
        self.button_start = tk.Button(self.root, text="Start",command=self.on_button_start_click,width=button_width, height=button_height)
        self.button_stats = tk.Button(self.root, text="Stats",command=self.on_button_stats_click,width=button_width, height=button_height)
        self.button_score_6 = tk.Button(self.root, text="Score = 6",command=self.on_button_6_click,width=button_width, height=button_height)
        self.button_score_5 = tk.Button(self.root, text="Score = 5",command=self.on_button_5_click,width=button_width, height=button_height)
        self.button_score_4 = tk.Button(self.root, text="Score = 4",command=self.on_button_4_click,width=button_width, height=button_height)
        self.button_score_3 = tk.Button(self.root, text="Score = 3",command=self.on_button_3_click,width=button_width, height=button_height)
        self.button_score_2 = tk.Button(self.root, text="Score = 2",command=self.on_button_2_click,width=button_width, height=button_height)
        self.button_score_1 = tk.Button(self.root, text="Score = 1",command=self.on_button_1_click, width=button_width, height=button_height)

        self.explanation_1 = tk.Button(self.root, text="?",command=self.on_button_1_exp_click,width=5, height=5)
        self.explanation_2 = tk.Button(self.root, text="?",command=self.on_button_2_exp_click,width=5, height=5)
        self.explanation_3 = tk.Button(self.root, text="?",command=self.on_button_3_exp_click,width=5, height=5)
        self.explanation_4 = tk.Button(self.root, text="?",command=self.on_button_4_exp_click,width=5, height=5)
        self.explanation_5 = tk.Button(self.root, text="?",command=self.on_button_5_exp_click,width=5, height=5)
        self.explanation_6 = tk.Button(self.root, text="?",command=self.on_button_6_exp_click,width=5, height=5)


        # Spacer-Spalte erstellen
        spacer = tk.Frame(self.root, width=100)
        spacer.grid(row=0, column=8, rowspan=7)
        spacer.grid(row=0, column=9, rowspan=7)
        spacer.grid(row=0, column=12, rowspan=7)
        
        self.button_exit.grid(row=3, column=13)
        self.button_start.grid(row=1, column=13)
        self.button_stats.grid(row=2, column=13)
        
        self.button_score_6.grid(row=6, column=10)
        self.button_score_5.grid(row=5, column=10)
        self.button_score_4.grid(row=4, column=10)
        self.button_score_3.grid(row=3, column=10)
        self.button_score_2.grid(row=2, column=10)
        self.button_score_1.grid(row=1, column=10)
        self.explanation_1.grid(row=1, column=11)
        self.explanation_2.grid(row=2, column=11)
        self.explanation_3.grid(row=3, column=11)
        self.explanation_4.grid(row=4, column=11)
        self.explanation_5.grid(row=5, column=11)
        self.explanation_6.grid(row=6, column=11)
        
        # Bind the event handler to the button
        self.root.bind('1', self.on_button_1_click)
        self.root.bind('2', self.on_button_2_click)
        self.root.bind('3', self.on_button_3_click)
        self.root.bind('4', self.on_button_4_click)
        self.root.bind('5', self.on_button_5_click)
        self.root.bind('6', self.on_button_6_click)


    def load_image(self):

        #Function to load a picture which gets provided by the opcua server
        self.server_class.node_username.set_value(self.user_name)
        self.server_class.send_message(f'image_{self.user_name}')
        time.sleep(0.1)
        image = self.server_class.give_me_pic()
        return image
    
        # Define event handlers (Buttons that you click)
   
    def on_button_start_click(self):
        self.image = self.load_image()
        self.image = Image.open(io.BytesIO(self.image))
        self.image = self.image.resize((self.image_width, self.image_height), Image.Resampling.LANCZOS)
        self.photo = ImageTk.PhotoImage(self.image)
        self.label = tk.Label(self.root, image=self.photo)
        self.label.grid(row=0, column=0,rowspan=7, columnspan=7)

    #recieving an csv_file from server containing all informations about already rated pictures 
    def on_button_stats_click(self): 
        self.server_class.send_message('csv_file')
        time.sleep(1)
        node_csv_file = self.server_class.client.get_node(f'ns={self.server_class.ns};s=csv_file')
        # Decode the byte string to a regular string
        csv_file = node_csv_file.get_value().decode("utf-8")

        # Split the CSV data into rows
        rows = csv_file.splitlines()

        # Open the output CSV file for writing
        with open("output.csv", "w", newline="", encoding="utf-8") as outfile:
            writer = csv.writer(outfile)

            # Iterate over the rows and write them to the output file
            for row in rows:
                writer.writerow(row.split(","))

    #Giving the picture that appears in the middle of the gui the score 1
    #After a new image gets loaded  
    def on_button_1_click(self,event=None):
        
        self.server_class.node_username.set_value(self.user_name)
        self.label.config(text="Score of picture: 1")
        node_score = self.server_class.client.get_node(f'ns={self.server_class.ns};s=image_score_{self.user_name}')
        node_score.set_value(1)
        self.server_class.send_message(f'score_{self.user_name}')
        with ThreadPoolExecutor() as executor:
            future = executor.submit(self.load_image)
            try:
                result = future.result(timeout=20)

                image = Image.open(io.BytesIO(result))
                image = image.resize((self.image_width, self.image_height), Image.Resampling.LANCZOS)
                new_photo = ImageTk.PhotoImage(image)
                self.label.image = new_photo
                self.label.config(image=new_photo)
                        
                if not result:
                    return None           
            
            except TimeoutError:
                print("Die Funktion hat die Zeitüberschreitung überschritten.")

        
    #Giving the picture that appears in the middle of the gui the score 2
    #After a new image gets loaded 
    def on_button_2_click(self,event=None):
        self.server_class.node_username.set_value(self.user_name)
        self.label.config(text="Score of picture: 2")
        node_score = self.server_class.client.get_node(f'ns={self.server_class.ns};s=image_score_{self.user_name}')
        node_score.set_value(2)
        self.server_class.send_message(f'score_{self.user_name}')
        with ThreadPoolExecutor() as executor:
            future = executor.submit(self.load_image)
            try:
                result = future.result(timeout=20)

                image = Image.open(io.BytesIO(result))
                image = image.resize((self.image_width, self.image_height), Image.Resampling.LANCZOS)
                new_photo = ImageTk.PhotoImage(image)
                self.label.image = new_photo
                self.label.config(image=new_photo)
                        
                if not result:
                    return None           
            
            except TimeoutError:
                print("Die Funktion hat die Zeitüberschreitung überschritten.")
         
    #Giving the picture that appears in the middle of the gui the score 3
    #After a new image gets loaded 
    def on_button_3_click(self,event=None):
        self.server_class.node_username.set_value(self.user_name)
        self.label.config(text="Score of picture: 3")
        node_score = self.server_class.client.get_node(f'ns={self.server_class.ns};s=image_score_{self.user_name}')
        node_score.set_value(3)
        self.server_class.send_message(f'score_{self.user_name}')
        with ThreadPoolExecutor() as executor:
            future = executor.submit(self.load_image)
            try:
                result = future.result(timeout=20)

                image = Image.open(io.BytesIO(result))
                image = image.resize((self.image_width, self.image_height), Image.Resampling.LANCZOS)
                new_photo = ImageTk.PhotoImage(image)
                self.label.image = new_photo
                self.label.config(image=new_photo)
                        
                if not result:
                    return None           
            
            except TimeoutError:
                print("Die Funktion hat die Zeitüberschreitung überschritten.")
        
        
    #Giving the picture that appears in the middle of the gui the score 4
    #After a new image gets loaded 
    def on_button_4_click(self,event=None):
        
        self.server_class.node_username.set_value(self.user_name)
        self.label.config(text="Score of picture: 4")
        node_score = self.server_class.client.get_node(f'ns={self.server_class.ns};s=image_score_{self.user_name}')
        node_score.set_value(4)
        self.server_class.send_message(f'score_{self.user_name}')
        with ThreadPoolExecutor() as executor:
            future = executor.submit(self.load_image)
            try:
                result = future.result(timeout=20)

                image = Image.open(io.BytesIO(result))
                image = image.resize((self.image_width, self.image_height), Image.Resampling.LANCZOS)
                new_photo = ImageTk.PhotoImage(image)
                self.label.image = new_photo
                self.label.config(image=new_photo)
                        
                if not result:
                    return None           
            
            except TimeoutError:
                print("Die Funktion hat die Zeitüberschreitung überschritten.")
        
    #Giving the picture that appears in the middle of the gui the score 5
    #After a new image gets loaded 
    def on_button_5_click(self,event=None):
        
        self.server_class.node_username.set_value(self.user_name)
        self.label.config(text="Score of picture: 5")
        node_score = self.server_class.client.get_node(f'ns={self.server_class.ns};s=image_score_{self.user_name}')
        node_score.set_value(5)
        self.server_class.send_message(f'score_{self.user_name}')
        with ThreadPoolExecutor() as executor:
            future = executor.submit(self.load_image)
            try:
                result = future.result(timeout=20)

                image = Image.open(io.BytesIO(result))
                image = image.resize((self.image_width, self.image_height), Image.Resampling.LANCZOS)
                new_photo = ImageTk.PhotoImage(image)
                self.label.image = new_photo
                self.label.config(image=new_photo)
                        
                if not result:
                    return None           
            
            except TimeoutError:
                print("Die Funktion hat die Zeitüberschreitung überschritten.")
        
    #Giving the picture that appears in the middle of the gui the score 6
    #After a new image gets loaded 
    def on_button_6_click(self,event=None):
        
        self.server_class.node_username.set_value(self.user_name)
        self.label.config(text="Score of picture: 6")
        node_score = self.server_class.client.get_node(f'ns={self.server_class.ns};s=image_score_{self.user_name}')
        node_score.set_value(6)
        self.server_class.send_message(f'score_{self.user_name}')
        with ThreadPoolExecutor() as executor:
            future = executor.submit(self.load_image)
            try:
                result = future.result(timeout=20)

                image = Image.open(io.BytesIO(result))
                image = image.resize((self.image_width, self.image_height), Image.Resampling.LANCZOS)
                new_photo = ImageTk.PhotoImage(image)
                self.label.image = new_photo
                self.label.config(image=new_photo)
                        
                if not result:
                    return None           
            
            except TimeoutError:
                print("Die Funktion hat die Zeitüberschreitung überschritten.")

    #Questionmark Buttons Explaining how to rate the pictures.      
    
    def on_button_1_exp_click(self):
            # Create a new window
        window = tk.Toplevel()
        window.geometry("600x100")
        window.title("Explanation")
        
        # Create a label with some text
        label = tk.Label(window, text="Objekt aus allen Perspektiven sehr gut erkennbar und ausgelichtet, Wenig Clutter, keine Occlusion")
        label.pack(pady=20)
    
    def on_button_2_exp_click(self):
            # Create a new window
        window = tk.Toplevel()
        window.geometry("600x100")
        window.title("Explanation")
        
        # Create a label with some text
        label = tk.Label(window, text="Gute Ausleuchtung, wenig Occlusion, Scharfe Abbildung, z.B. anderer Hintergrund, Occlusion < 10 %")
        label.pack(pady=20)
    
    def on_button_3_exp_click(self):     
          # Create a new window
        window = tk.Toplevel()
        window.geometry("800x100")
        window.title("Explanation")
        
        # Create a label with some text
        label = tk.Label(window, text="Überstrahlung, wenig Kontrast (dunkles Objekt), Occlusion <25 % ok, solange wichtige Merkmale passt, Oberfläche eingeschränkt erkennbar,")
        label.pack(pady=20)
    
    def on_button_4_exp_click(self):
   
          # Create a new window
        window = tk.Toplevel()
        window.geometry("600x100")
        window.title("Explanation")
        
        # Create a label with some text
        label = tk.Label(window, text="Objekt sichtbar aber Artefakte beeinflussen das Bild, > 50 % sichtbar, Oberflächenstruktur ist nicht erkennbar")
        label.pack(pady=20)
    
    def on_button_5_exp_click(self):
    
           # Create a new window
        window = tk.Toplevel()
        window.geometry("600x100")
        window.title("Explanation")
        
        # Create a label with some text
        label = tk.Label(window, text="Man könnte was lernen aber die KI muss es nicht erkennen können. Nutzerfehler <50 % zu sehen")
        label.pack(pady=20)
    
    def on_button_6_exp_click(self):

        # Create a new window
        window = tk.Toplevel()
        window.geometry("600x100")
        window.title("Explanation")
        
        # Create a label with some text
        label = tk.Label(window, text="Es gibt absolut nichts zu einem Objekt zu lernen")
        label.pack(pady=20)
    
    #Button to leave the application. Either leave with exit Button or Cross in up top left corner. 
    def exit_program(self):
        
        user_array_node = self.server_class.client.get_node(f'ns={self.server_class.ns};s=username_array')
        user_array = user_array_node.get_value()
        
        if self.user_name in user_array:
            user_array.remove(self.user_name)
        
        if not user_array:
            user_array_node.set_value([], varianttype=ua.VariantType.Null)
        else:
            user_array_node.set_value(user_array)

        self.root.destroy()
        self.server_class.client.disconnect()

    class ServiceClient:
        
        # opcua Client class.
        # connecting to the opcua server 
        
        def __init__(self, user_name, with_prints=False):
            
            self.user_name = user_name
            self.with_prints = with_prints
            #ip = '172.17.21.69'
            ip = '127.0.0.1'
            port = '9000'
            name_space = 'Image_Rating'
            con = f'opc.tcp://{ip}:{port}'
        
            if self.with_prints:
                print(f'ip {ip}')
                print(f'port {port}')
                print(f'name_space {name_space}')
                print(f'connection: {con}')

            self.client = Client(con)
            self.client.connect()
            self.ns = self.client.get_namespace_index(name_space)
            print('Client Connected')
            print("name space index ", self.ns)
            user_array = self.client.get_node(f'ns={self.ns};s=username_array')
            node_value = user_array.get_value()  # den aktuellen Wert des Arrays abrufen
            my_array = list(node_value)  # in eine Python-Liste umwandeln
            my_array.append(f'{self.user_name}')  # ein neues Element zum Array hinzufügen
            new_value = ua.Variant(my_array, ua.VariantType.String)  # das aktualisierte Array in einen OPC UA-Varianten umwandeln
            user_array.set_value(new_value)  # die aktualisierte Node-Variable speichern
            self.node_username = self.client.get_node(f'ns={self.ns};s=username')

        def send_message(self, string):
            
            state_changer = self.client.get_node(f'ns={self.ns};s=state')
            state_changer.set_value(string)

        def give_me_pic(self):

            image_data = self.client.get_node(f'ns={self.ns};s=image_data_{self.user_name}')
            image = image_data.get_value() 
            return image
        
if __name__ == '__main__':
    
    user_name = input("Bitte gib hier deinen Namen ein? ")
    graphic_user_interface = GUI(user_name)
    graphic_user_interface.root.mainloop()

    
    