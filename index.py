import datetime
import time
import logging
import pandas as pd
import xml.etree.ElementTree as ET
from potatoscript.potatoConfig import Config as config
from potatoscript.potatoEmail import Config as email
from colorama import init, Fore, Back, Style
import subprocess
import ctypes
import subprocess

class Check:
    def __init__(self):

        self.in_process_flag = False

        # 設定の読み込み
        self.config = config('config.ini')
        self.email_config = email(self.config.get('EMAIL', 'smtp_server'),
                                   self.config.get('EMAIL', 'smtp_port'),
                                   self.config.get('EMAIL', 'sender'))
        self.NG_EMAIL_subject = self.config.get('NG_EMAIL', 'subject')
        self.NG_EMAIL_message_header = self.config.get('NG_EMAIL', 'message_header')
        self.NG_EMAIL_message_header2 = self.config.get('NG_EMAIL', 'message_header2')
        self.NG_EMAIL_message_footer = self.config.get('NG_EMAIL', 'message_footer')
        self.NG_EMAIL_message_footer2 = self.config.get('NG_EMAIL', 'message_footer2')
        self.NG_EMAIL_recipient = self.config.get('NG_EMAIL', 'recipient')
        self.NG_EMAIL_recipient_cc = self.config.get('NG_EMAIL', 'recipient_cc')
        
        # その他の変数の初期化
        self.console_title = self.config.get('PARAM', 'console_title')
        self.csv_file_path = self.config.get('PARAM', 'csv_file_path')
        self.checked_file = self.config.get('PARAM', 'checked_file')
        self.system_in_process = self.config.get('PARAM', 'system_in_process')
        self.namespace = self.config.get('PARAM', 'namespace')
        self.interval = self.config.get('PARAM', 'interval')
        
        # SharePoint File Uploadの変数の初期化
        self.UPLOAD_EMAIL_recipient = self.config.get('UPLOAD_EMAIL', 'recipient')
        self.UPLOAD_EMAIL_recipient_cc = self.config.get('UPLOAD_EMAIL', 'recipient_cc')
        self.UPLOAD_EMAIL_subject = self.config.get('UPLOAD_EMAIL', 'subject')
        self.UPLOAD_EMAIL_message_header = self.config.get('UPLOAD_EMAIL', 'message_header')
        self.UPLOAD_EMAIL_message_header2 = self.config.get('UPLOAD_EMAIL', 'message_header2')
        self.UPLOAD_EMAIL_message_footer = self.config.get('UPLOAD_EMAIL', 'message_footer')
        self.UPLOAD_EMAIL_message_footer2 = self.config.get('UPLOAD_EMAIL', 'message_footer2')
        self.UPLOAD_PARAM_file_paths = self.config.get('UPLOAD_PARAM', 'file_paths')
        self.UPLOAD_PARAM_app_dir = self.config.get('UPLOAD_PARAM', 'app_dir')
        self.UPLOAD_PARAM_checked_file = self.config.get('UPLOAD_PARAM', 'checked_file')

        # Define the expected columns from the config.ini
        self.CoC_expected_columns = self.get_expected_columns()

        console_handle = ctypes.windll.kernel32.GetConsoleWindow()
        ctypes.windll.kernel32.SetConsoleTitleW(self.console_title)

        self.start_sharepoint_upload = 0

        # チェックのためのリストの初期化
        self.waferIds, self.lotnos, self.totalDies, self.goodDies = [], [], [], []

        init(autoreset=True)                                            # autoreset=Trueを設定して、Coloramaのテキストリセットを有効にします。
        logging.basicConfig(filename='record.log', level=logging.INFO)  # ロギングを設定し、'record.log' ファイルにログを記録します。  


    def get_expected_columns(self):
        return [self.config.get('COC', str(i)) for i in range(1, 199)]

    def run(self):
        while True:
            print("システムを起動しています")
            if not self.in_process_flag:
                # Set the flag to indicate the process is ongoing
                self.in_process_flag = True

                try:
                    self.read_emailBoxNo()  # check the existence of uploaded boxno to send email
                    self.main()
                finally:
                    # Reset the flag after main() completes
                    self.in_process_flag = False  

            interval = int(self.interval)  # Replace with your desired interval
            time.sleep(interval)    

    def main(self):
        
        root_folder = r"" + self.csv_file_path
        new_boxno = 0
        for folder_name in os.listdir(root_folder):
            error_message = []
            folder_path = os.path.join(root_folder, folder_name)
            boxno = folder_name
            checked_file_path = os.path.join(folder_path, self.checked_file)
            subfolder_path = os.path.join(folder_path, '出荷編成情報.csv')
            
            if not os.path.isdir(folder_path) or os.path.exists(checked_file_path) or not os.path.exists(subfolder_path):
                continue

            new_boxno = 1

            now = datetime.datetime.now()
            # 現在の日付とBOX NOを表示
            print(f"{Fore.WHITE}{Back.YELLOW}{Style.BRIGHT}{now.strftime('%Y-%m-%d')}　{Back.BLUE}BOX NO: {boxno}{Style.NORMAL}")

            df = pd.read_csv(subfolder_path, index_col='item')
            
            WaferId_key = [f'RWID-{i:02d}' for i in range(1, 14)]
            LotNo_key = [f'RWID_FAB_WF_ID-{i:02d}' for i in range(1, 14)]
            TotalDie_key = [f'RWID_TOTAL_DIE-{i:02d}' for i in range(1, 14)]
            GoodDie_key = [f'RWID_GOOD_DIE-{i:02d}' for i in range(1, 14)]
            self.itemName = df.loc['ITEM_NAME', 'value']
            
            for key, key2, key3, key4 in zip(WaferId_key, LotNo_key, TotalDie_key, GoodDie_key):
                WaferId = df.loc[key, 'value'] if key in df.index else None
                LotNo = df.loc[key2, 'value'] if key2 in df.index else None
                TotalDie = df.loc[key3, 'value'] if key3 in df.index else None
                GoodDie = df.loc[key4, 'value'] if key4 in df.index else None
                
                if isinstance(WaferId, str) and WaferId is not None and WaferId != 'nan' and WaferId.startswith('PQ'):
                    # PQで始まるWaferIdをリストに追加
                    self.waferIds.append(WaferId)
                    self.lotnos.append(LotNo)
                    self.totalDies.append(TotalDie)
                    self.goodDies.append(GoodDie)

                    folder_to_check = os.path.join(folder_path, WaferId)
                    
                    if os.path.exists(folder_to_check) and os.path.isdir(folder_to_check):
                        # ディレクトリ '{WaferId}' が存在します。
                        print(f"The directory '{WaferId}' 存在です.")
                    else:
                        # ディレクトリ '{WaferId}' は存在しません。
                        print(f"{Fore.WHITE}{Back.RED}{Style.BRIGHT}The directory '{WaferId}' does not exist.{Style.NORMAL}")

            for subfolder_name in os.listdir(folder_path):
                subfolder_path = os.path.join(folder_path, subfolder_name)
                
                if not os.path.isdir(subfolder_path):
                    continue

                elif folder_name in subfolder_name or subfolder_name == "Output_Maps":
                    for file_name in os.listdir(subfolder_path):
                        file_path = os.path.join(subfolder_path, file_name)
                        file_name = os.path.basename(file_path)
                        result = None
                        
                        if file_name.endswith(".csv") and "_TMAP" in file_name.upper():
                            # TMAPファイルのチェックを実行
                            result = self.tMap_check_result(file_path, file_name, boxno)
                        elif file_name.endswith(".csv") and "_COC" in file_name.upper():
                            # COCファイルのチェックを実行
                            result = self.CoC_check_result(file_path, file_name, boxno)
                        elif file_name.endswith(".xml"):
                            # eMAPファイルのチェックを実行
                            result = self.eMap_check_result(file_path, file_name, boxno)

                        if result is not None:
                            ng = result
                            if ng is not None and file_name is not None:
                                error_message.append((boxno, file_name, ng))
                    
            if len(error_message) > 0:
                self.start_sharepoint_upload = 1
                self.send_alert_email(error_message)    # エラーメッセージがある場合、アラートメールを送信
                checked_file_path = os.path.join(folder_path, self.checked_file)
                with open(checked_file_path, "w") as check_file:
                    check_file.write(f"TMAP、EMAP、COCにエラーが見つかりました。{error_message}")  # TMAP、EMAP、COCにエラーは見つかりませんでした。
            else:
                checked_file_path = os.path.join(folder_path, self.checked_file)
                with open(checked_file_path, "w") as check_file:
                    check_file.write("TMAP、EMAP、COCにエラーは見つかりませんでした。")  # TMAP、EMAP、COCにエラーは見つかりませんでした。

            print('')


        if self.start_sharepoint_upload == 0 and new_boxno == 1:
            # File Upload to sharepoint    
            # Only run FileUploader when len(error_message) == 0
            self.execute_sharepoint_upload()  

    # def tMap_check_result(self, file_path, file_name, boxno):
    #     try:
    #         pd.read_csv(file_path)
    #         self.printInfo(boxno, file_name, "OK")      # tMapファイルが正常に読み込まれました。エラーメッセージはありません。
    #         return None
    #     except pd.errors.ParserError as e:
    #         ng = e.args[0].split(":")[-1].strip()
    #         self.printInfo(boxno, file_name, ng)        # tMapファイル '{file_name}' のパース中にエラーが発生しました: {ng}
    #         return ng
    def tMap_check_result(self, file_path, file_name, boxno):
        try:
            # Read the CSV with dtype specified for each column
            df = pd.read_csv(file_path, dtype=self.get_column_types(file_path))
            self.printInfo(boxno, file_name, "OK")  # tMapファイルが正常に読み込まれました。エラーメッセージはありません。
            return None
        except pd.errors.ParserError as e:
            ng = f"{file_name} のパース中にエラーが発生しました: {e}"
            self.printInfo(boxno, file_name, ng)  # tMapファイル '{file_name}' のパース中にエラーが発生しました: {ng}
            return ng
        except Exception as e:
            ng = f"{file_name} のデータ型が異なる列があります: {e}"
            self.printInfo(boxno, file_name, ng)  # tMapファイル '{file_name}' のデータ型が異なる列があります: {ng}
            return ng
        
    def CoC_check_result(self, file_path, file_name, boxno):

         # Read the CSV with dtype specified for each column
        df = pd.read_csv(file_path)

        # Check if values in column A match the expected values
        expected_values = self.CoC_expected_columns  # Assuming column A is the first column
        actual_values = df[df.columns[0]].tolist()  # Get values in the first column of the CSV
        if expected_values == actual_values:
            self.printInfo(boxno, file_name, "OK")  # CoCファイルが正常に読み込まれました。エラーメッセージはありません。
            return None
        else:
            ng = f"{file_name} の列が期待される列と一致しません"
            self.printInfo(boxno, file_name, ng)  # CoCファイル '{file_name}' の列が期待される列と一致しません: {ng}
            return ng


    def get_column_types(self, file_path):
        # Read the first few rows to infer column types
        sample_size = 5
        df_sample = pd.read_csv(file_path, nrows=sample_size)

        # Get data types of each column
        column_types = df_sample.dtypes.to_dict()

        return column_types

    def eMap_check_result(self, file_path, file_name, boxno):
        try:
            tree = ET.parse(file_path)
            root = tree.getroot()
            namespace = {'ns': self.namespace}
            bin_codes = ['A', 'X', 'W', 'Z']
            ng = []

            WaferId = root.get('WaferId')                                   # XMLファイルのルート要素から 'WaferId' 属性を抽出します。
            device_element = root.find(".//{http://www.semi.org}Device")    # 適切な名前空間を使用して、XMLファイル内の 'Device' 要素を検索します。
            ProductId = device_element.get("ProductId")                     # 'Device' 要素から 'ProductId' と 'LotId' 属性を抽出します。
            LotId = device_element.get("LotId")

            wafer_id_to_find = file_name.split('.')[0]
            index = self.waferIds.index(wafer_id_to_find)
            if WaferId != wafer_id_to_find:
                ng.append("Wafer Id not match")             # Wafer Idが一致しません
            if LotId != wafer_id_to_find[:10]:
                ng.append("Lot Id not match")               # Lot Idが一致しません
            if ProductId != self.itemName.split('-')[0]:
                ng.append("Product Id not match")           # Product Idが一致しません

            lotno = self.lotnos[index]
            total_die = self.totalDies[index]
            check_total_die = 0

            for bin_code in bin_codes:
                bin_element = root.findall(f".//ns:Bin[@BinCode='{bin_code}']", namespaces=namespace)
                result = self.checkBinCount(bin_code, bin_element, root, namespace, index)
                msg, qty = result

                if bin_code != "Z" and bin_code != "W":
                    check_total_die += int(qty)

                if msg != 'OK':
                    ng.append(result)

            bin_elements = root.findall(f".//ns:Bin[@BinCode='A']", namespaces=namespace)
            for bin_element in bin_elements:
                if ';' in lotno:
                    split_lotnos = lotno.split(';')  # ';' でLotNoを分割
                    for split_lotno in split_lotnos:
                        if split_lotno.strip() not in bin_element.get("BinDescription"):
                            # LotNo '{split_lotno}' はBinDescriptionに見つかりません
                            print(f"　　{Fore.WHITE}{Back.RED}{Style.BRIGHT}LotNo '{split_lotno}' not found in BinDescription{Style.NORMAL}")
                            ng.append(f"LotNo '{split_lotno}' not found in BinDescription<br>")
                else:
                    if lotno.strip() not in bin_element.get("BinDescription"):
                        # LotNo '{lotno}' はBinDescriptionに見つかりません
                        print(f"　　{Fore.WHITE}{Back.RED}{Style.BRIGHT}LotNo '{lotno}' not found in BinDescription{Style.NORMAL}")
                        ng.append(f"LotNo '{lotno}' not found in BinDescription<br>")

            if check_total_die != int(total_die):
                # 総数 ({total_die}) と合計数 ({check_total_die})
                print(f"　　{Fore.WHITE}{Back.RED}{Style.BRIGHT}編成情報のTOTAL_DIE ({total_die}) と合計DIE ({check_total_die}{Style.NORMAL}")
                ng.append(f" 編成情報のTOTAL_DIE ({total_die}) と合計DIE ({check_total_die}<br>")

            if not ng:
                # eMapファイルが正常です。エラーメッセージはありません。
                self.printInfo(boxno, file_name, "OK")
                return None
            else:
                # eMapファイル '{file_name}' にNG項目があります。詳細: {ng}
                self.printInfo(boxno, file_name, ng)
                return ng

        except ET.ParseError:
            print(f"XMLファイルの解析中にエラーが発生しました: {file_path}")
        except ValueError:
            print("BinCountを整数に変換中にエラーが発生しました。")

    def checkBinCount(self, target, bin_elements, root, namespace, index):
        good_die = self.goodDies[index]
        if bin_elements is not None:
            total_bin_count = 0
            for bin_element in bin_elements:
                if bin_element is not None:
                    bin_count = int(bin_element.get("BinCount"))
                    total_bin_count += bin_count

            row_count = 0
            for row_element in root.findall(".//ns:Row", namespaces=namespace):
                row_data = row_element.text
                a_count = row_data.count(target)
                row_count += a_count

            if target == 'A' and int(good_die) != total_bin_count:
                # QTY 'A'文字 ({good_die}) != BinCount ({total_bin_count})
                print(f"　　{Fore.WHITE}{Back.RED}{Style.BRIGHT}QTY '{target}' characters ({good_die}) != BinCount ({total_bin_count}){Style.NORMAL}")
                return (f"QTY '{target}' ({good_die}) != BinCount ({total_bin_count}<br>", total_bin_count)

            if row_count == total_bin_count:
                # QTY 'A'文字 == BinCount: {total_bin_count}
                print(f"　　QTY '{target}' characters == BinCount: {total_bin_count}")
                return ('OK', total_bin_count)
            else:
                # QTY 'A'文字 ({row_count}) != BinCount ({total_bin_count})
                print(f"　　{Fore.WHITE}{Back.RED}{Style.BRIGHT}QTY '{target}' characters ({row_count}) != BinCount ({total_bin_count}){Style.NORMAL}")
                return (f"QTY '{target}' ({row_count}) != BinCount ({total_bin_count}<br>", total_bin_count)
        else:
            # BinCode='{target}' のBin要素がXML内で見つかりませんでした。
            print(f"Bin element with BinCode='{target}' not found in the XML.")

    def printInfo(self, boxno, csv_file_name, info):
        now = datetime.datetime.now()
        if info == "OK":
            message = f"　　{Fore.WHITE}{Back.YELLOW}{Style.BRIGHT}{now.strftime('%H:%M:%S')}{Back.BLACK} BoxNo: {boxno}  File: {csv_file_name} - OK{Style.NORMAL}"
        else:
            message = f"　　{Fore.WHITE}{Back.YELLOW}{Style.BRIGHT}{now.strftime('%H:%M:%S')}{Fore.WHITE}{Back.RED}{Style.BRIGHT} BoxNo: {boxno} File: {csv_file_name} - NG　{info}{Style.NORMAL}"
        print(message)
        print('-----------------------------------------------------------------------------')
        logging.info(message)

    def send_alert_email(self, error_message):
        header = f"{self.NG_EMAIL_message_header}<br>{self.NG_EMAIL_message_header2}"
        footer = f"{self.NG_EMAIL_message_footer}<br>{self.NG_EMAIL_message_footer2}"

        body = """
        <table style="border-collapse: collapse; border: 1px solid black;">
        <tr>
            <th style="padding: 8px; text-align: left; border: 1px solid black;">Box No</th>
            <th style="padding: 8px; text-align: left; border: 1px solid black;">File Name</th>
            <th style="padding: 8px; text-align: left; border: 1px solid black;">ERROR</th>
        </tr>
        """
        for m in error_message:
            body += f"""
            <tr>
            <td style="padding: 8px; text-align: left; border: 1px solid black;">{m[0]}</td>
            <td style="padding: 8px; text-align: left; border: 1px solid black;">{m[1]}</td>
            <td style="padding: 8px; text-align: left; border: 1px solid black;">{m[2]}</td>
            </tr>
            """

        body += "</table><br>"

        recipient = self.NG_EMAIL_recipient.split(';')
        recipient_cc = self.NG_EMAIL_recipient_cc.split(';')
        self.email_config.send(self.NG_EMAIL_subject, header, body, footer, recipient, recipient_cc)

    def read_emailBoxNo(self):
        folder_path = r""+self.UPLOAD_PARAM_file_paths
        os.listdir(folder_path)
        #os.chdir(folder_path)

        if self.check_folder_not_empty():
            for f in os.listdir():
                file_name = os.path.basename(f)
                base_name, extension = os.path.splitext(file_name)   
                self.send_notification_email(base_name) # send the email for the uploaded box no 
                os.remove(file_name)                    # delete the sent boxno.txt


    def check_folder_not_empty(self):
        files_in_folder = [f for f in os.listdir() if os.path.isfile(f)]
        txt_files = [f for f in files_in_folder if f.lower().endswith('.txt')]
        if txt_files:
            return True
        else:
            return False

    def execute_sharepoint_upload(self):
        try:
            subprocess.run(r"" + self.UPLOAD_PARAM_app_dir, shell=True, check=True)
            now = datetime.datetime.now()
            print(f'"{now.strftime("%Y-%m-%d %H:%M:%S")} File uploading completed.')
        except FileNotFoundError:
            print("C# application not found.")
        except subprocess.CalledProcessError as e:
            print(f"Error occurred while opening C# application: {e}")
        except Exception as e:
            print(f"Unexpected error occurred: {e}")
        finally:
            # Reset the flag after the C# process completes or encounters an error
            self.in_process_flag = False

    def send_notification_email(self, file_name):
        recipient = self.UPLOAD_EMAIL_recipient.split(';')
        recipient_cc = self.UPLOAD_EMAIL_recipient_cc.split(';')
        subject = self.UPLOAD_EMAIL_subject + " " + file_name
        message_header = f"{self.UPLOAD_EMAIL_message_header}<br><br>"
        message_header += f"<font style='color:blue;font-weight:bold;font-size:18px'>Box No : {file_name} {self.UPLOAD_EMAIL_message_header2}</font><br><br>"
        message_footer = f"<br><br>{self.UPLOAD_EMAIL_message_footer}<br><br>{self.UPLOAD_EMAIL_message_footer2}"
        self.email_config.send(subject, message_header,"", message_footer, recipient, recipient_cc)

if __name__ == "__main__":
    process = Check()
    process.run()
