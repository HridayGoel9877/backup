from pyarrow import parquet
import gcsfs
import pandas as pd
from openpyxl import workbook
import json

def main():

    def getConfig():
        with open("config.json","r") as f:
            conf = json.load(f)
        return conf

    config=getConfig()


    fs = gcsfs.GCSFileSystem(project=config['project'], token=config['token'])

    parqfile = parquet.ParquetFile("gs://gcp_casestudy_bucket/Data/2024-08-02/CAN3BS6/cos-to-gcs_files-decrypted_edt=20240802_pkt=CAN3BS6_0_20240918_4716640.parquet", filesystem=fs)
    #parqfile = parquet.ParquetFile(parqfile)
    # schema= parqfile.schema
    # print(schema)
    # print(parqfile.metadata.num_columns)

    datesPath = config["dates_path"]
    # files = fs.glob(dates_path)
    # print(files)

    packetsPath = config["packets_path"]

    #PRINTING DATES
    dates = [item.split('/')[-1] for item in fs.glob(datesPath)]  
    #print(date)

    # date=[]
    # for item in fs.glob(dates_path):
    #     date.append(item.split('/')[-1])
    # print(date)


    #PRINTING PACKAGES
    packets=[item.split('/')[-1] for item in fs.glob(packetsPath)]  
    packets= list(set(packets))
    #print(packets)

    # package=[]
    # for item in fs.glob(package_path):
    #     package.append(item.split('/')[-1])
    # print(package)

    #INPUT DATES AND PACKETS
    startDate=config['start_date']
    endDate=config['end_date']
    inputPackets=config['input_packets']

    #DATE FILTERING
    new_dates=[]
    for date in dates:    
        if date >= startDate and date<=endDate:
            new_dates.append(date)
        elif date>=startDate and endDate=='':
            new_dates.append(date)

    #PACKAGE FILTERING
    if not inputPackets:
        new_packets=packets
    else:
        new_packets=[]
        for packet in packets:
            if packet in inputPackets:
                new_packets.append(packet) 

    # print(new_dates)
    # print(new_packets)

    #COUNT ROWS
    total_records=[]
    for date in new_dates:
        for packet in new_packets:

            total=0
            for paths in fs.glob(f"gs://gcp_casestudy_bucket/Data/{date}/{packet}/*.parquet"):   
                parqfile = parquet.ParquetFile(paths, filesystem=fs)
                total+=parqfile.metadata.num_rows
                # print(parqfile.metadata.num_rows)
            #print(f'Date: {date}, Packet: {packet} Total: {total}')

            t1=(date,packet,total)
            total_records.append(t1)

    dataframe=pd.DataFrame(total_records, columns=['date','type','count'])
    #print(dataframe)

    df= dataframe.pivot(index='type',columns='date',values='count')
    print(df)

    #df.to_excel('total_records.xlsx')

if __name__=="__main__":
    main()