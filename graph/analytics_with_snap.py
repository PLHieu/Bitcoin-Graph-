import snap
import pandas as pd
import sys
from csv import reader
import os
import signal
import math

sys.path.append(os.path.abspath(os.path.abspath(os.path.dirname("__file__"))))
from utils.db import db
from mgoqueue import queue


received_signal = False

def signal_handler(signal, frame):
    global received_signal
    print("signal received", signal)
    received_signal = True

signal.signal(signal.SIGINT, signal_handler)
col_famous_address = db['famous_address']
col_famous_address_txns_v2 = db['famous_address_txns_v2']
col_feature_with_time = db['feature_with_time']
col_feature_partition = db['feature_partition']
col_famous_inoutflow_v2 = db['famous_inoutflow_v2']

def analytics_with_snap_big_graph():
    # # load the graph into snap first
    # G = snap.TUNGraph.New()

    # # load the graph from csv file 
    # df = pd.read_csv("processed_set_edges.csv",header=None)

    # for row in df.itertuples():
    #     if received_signal:
    #         break

    #     from_node = row._1
    #     to_node = row._2

    #     if not G.IsNode(from_node):
    #         G.AddNode(from_node)
        
    #     if not G.IsNode(to_node):
    #         G.AddNode(to_node)

    #     if from_node != to_node:
    #         G.AddEdge(from_node, to_node)


    # print("Information of deg of nodes in this partition")
    # DegToCntV = G.GetDegCnt()
    # with open(f'deg_info_big_big_graph.txt','w') as file:
    #     for item in DegToCntV:
    #         line = f"{item.GetVal2()} nodes with degree {item.GetVal1()}\n"
    #         file.write(line)


    # doc tu file deg_info_big_big... roi draw chart 
    with open('deg_info_big_big_graph.txt', 'r') as read_obj:
        i = 0

        list_range = [(1,5), (6,20), (21,100), (101,500),(501,1000),(1001,1301325)]
        data = []
        for row in read_obj:
            row = row.strip('\n') 
            pair_grs = row.split(' ')
            int_pair_grs = [ int(i) for i in pair_grs]
            data.append(int_pair_grs)

        for range in list_range:
            from_ = range[0]
            to = range[1]
            total = 0
            for pair in data:
                if pair[0] >= from_ and pair[0] <= to:
                    total += pair[1]
            print(f'({from_}-{to},{total})') 
        
        for range in list_range:
            from_ = range[0]
            to = range[1]
            total = 0
            print(f'{from_}-{to}',sep=', ', end=', ')
        
def get_set_address_by_partition(par):
    # load the graph from csv file 
    df = pd.read_csv("result_no_index.csv")
    result = set()

    for row in df.itertuples():
        if row.partition != par:
            continue
        node = row.node
        result.add(node)
    
    return result

def get_map_address_partition():
    # load the graph from csv file 
    df = pd.read_csv("result_no_index.csv")
    result = dict()

    for row in df.itertuples():
        result[row.node] = row.partition
    
    return result

def get_map_address_label():
    print("get_map_address_label")
    offset = 0
    limit = 1000000
    result = dict()
    while True:
        res = list(col_famous_address.find().skip(offset).limit(limit))
        if len(res) == 0:
            break

        for item in res:
            result[item.get("address","")] = item.get("label")

        offset += limit
    print("Done get_map_address_label")
    return result

def count_total_inout_in_partition():
    map_address_partition = get_map_address_partition()
    map_address_label = get_map_address_label()
    result_in = dict()
    result_out = dict()

    limit = 5000000
    offset = 0

    print("start counting total inio")
    while True:
        res = list(col_famous_inoutflow_v2.find().skip(offset).limit(limit))
        if len(res) == 0:
            break

        for item in res:
            add = item.get("address","") 
            label = map_address_label.get(add) 
            partition = map_address_partition.get(label)

            if partition is None:
                continue

            amount = item.get("amount",0)
            if item.get("type","") == "INFLOW":
                result_in[partition] = result_in.get(partition, 0) + amount
            if item.get("type","") == "OUTFLOW":
                result_out[partition] = result_out.get(partition, 0) + amount

        print(offset)
        offset += limit

        if received_signal:
            break
    print("end counting total inio")
    

    for par, total in result_in.items():
        item = col_feature_partition.find_one({"partition": par})
        num_node = item.get("num_node")
        mean_all_in_among_node = total/num_node
        col_feature_partition.update_one({"partition": par}, {"$set": {"total_in": total, "mean_all_in_among_node": mean_all_in_among_node}})
    
    for par, total in result_out.items():
        item = col_feature_partition.find_one({"partition": par})
        num_node = item.get("num_node")
        mean_all_out_among_node = total/num_node
        col_feature_partition.update_one({"partition": par}, {"$set": {"total_out": total, "mean_all_out_among_node": mean_all_out_among_node}})

def analytics_with_snap(partition):
    # firstly, load the set address in this partition
    set_adds = get_set_address_by_partition(partition)

    # load the graph into snap first
    G = snap.TUNGraph.New()

    # load the graph from csv file 
    df = pd.read_csv("processed_set_edges.csv",header=None)

    for row in df.itertuples():
        if received_signal:
            break

        from_node = row._1
        to_node = row._2

        if not (from_node in set_adds):
            continue  

        if not (to_node in set_adds):
            continue  

        if not G.IsNode(from_node):
            G.AddNode(from_node)
        
        if not G.IsNode(to_node):
            G.AddNode(to_node)

        if from_node != to_node:
            G.AddEdge(from_node, to_node)
          

    print("General info of partition", partition)
    G.PrintInfo()

    print("Information of deg of nodes in this partition")
    DegToCntV = G.GetDegCnt()
    with open(f'deg_info_par_{partition}.txt','w') as file:
        for item in DegToCntV:
            line = f"{item.GetVal2()} nodes with degree {item.GetVal1()}\n"
            file.write(line)
    

    print("Start getting all feature") 
    list_feature = []
    list_ath_in = []
    list_ath_out = []
    list_low_in = []
    list_low_out = []
    list_first_date = []
    list_last_date = []
    offset = 0
    limit = 1000000
    while True:
        res = list(col_feature_with_time.find().skip(offset).limit(limit))
        if len(res) == 0:
            break
        
        for item in res:
            if item.get("label") in set_adds:
                list_feature.append(item)
                if not math.isnan(item.get("ath_outflow",0)):
                    list_ath_out.append(item.get("ath_outflow",0))
                if not math.isnan(item.get("ath_inflow",0)):
                    list_ath_in.append(item.get("ath_inflow",0))
                if not math.isnan(item.get("lowest_outflow",math.inf)):
                    list_low_out.append(item.get("lowest_outflow",math.inf))
                if not math.isnan(item.get("lowest_inflow",math.inf)):
                    list_low_in.append(item.get("lowest_inflow",math.inf))
                if item.get("first_date_txn"):
                    list_first_date.append(item.get("first_date_txn"))
                if item.get("last_date_txn"):
                    list_last_date.append(item.get("last_date_txn"))

        offset += limit
        if received_signal:
            break
    print("End getting all feature") 

    len_l = len(list_feature)
    
    print("find number of txn")
    num_txns = 0
    for item in list_feature:
        num_txns += item.get("num_txns")

    print("find ath outflow")
    # sorted_outflow = sorted(list_feature, key=lambda item: float('-inf') if  item.get("ath_outflow", 0))
    # ath_outflow = sorted_outflow[len_l -1].get("ath_outflow")
    sorted_outflow = sorted(list_ath_out)
    ath_outflow = sorted_outflow[len(sorted_outflow) -1]

    print("find lowest outflow")
    # sorted_low_outflow = sorted(list_feature, key=lambda item: item.get("lowest_outflow", 0))
    # lowest_outflow = sorted_low_outflow[0].get("lowest_outflow")
    sorted_low_outflow = sorted(list_low_out)
    lowest_outflow = sorted_low_outflow[0]

    print("find ath inflow")
    # sorted_high_inflow = sorted(list_feature, key=lambda item: item.get("ath_inflow", 0))
    # ath_inflow = sorted_outflow[len_l -1].get("ath_inflow")
    sorted_high_inflow = sorted(list_ath_in)
    ath_inflow = sorted_high_inflow[len(sorted_high_inflow) -1]

    print("find lowest inflow")
    # sorted_low_inflow = sorted(list_feature, key=lambda item: item.get("lowest_inflow", 0))
    # lowest_inflow = sorted_low_inflow[0].get("lowest_inflow")
    sorted_low_inflow = sorted(list_low_in)
    lowest_inflow = sorted_low_inflow[0]

    # sorted_first_date = sorted(list_feature, key=lambda item: item.get("first_date_txn"))
    # first_date_txn = sorted_first_date[0].get("first_date_txn")
    sorted_first_date = sorted(list_first_date)
    first_date_txn = sorted_first_date[0]

    # sorted_last_date = sorted(list_feature, key=lambda item: item.get("last_date_txn"))
    # last_date_txn = sorted_last_date[len_l-1].get("last_date_txn")
    sorted_last_date = sorted(list_last_date)
    last_date_txn = sorted_last_date[len(sorted_last_date) -1]

    # df = pd.read_csv("data_per_week_for_predict.csv")
    total_in = 0
    total_out = 0
    total_in_plus_out = 0
    total_in_minus_out = 0
    # for _, row in df.iterrows():
    #     total_in_plus_out += row[f"{0 + partition*3}"]
    #     total_in_minus_out += row[f"{2 + partition*3}"]

    # total_in =(total_in_plus_out + total_in_minus_out)/2
    # total_out = total_in_plus_out - total_in

    print("num node")
    num_node = len(set_adds)

    print("num_edge")
    num_edge = G.CntUniqUndirEdges()

    # print("caculate mean all in among node")
    # mean_all_in_among_node = total_in/num_node

    # print("caculate mean out in among node")
    # mean_all_out_among_node = total_out/num_node


    mean_mean_out = 0
    mean_mean_in = 0
    div_in = 0
    div_out = 0
    for item in list_feature:
        res_in = item.get("mean_inflow")
        res_out = item.get("mean_outflow")
        if res_in and not (math.isnan(res_in)) and res_in != 0  :
            mean_mean_in += res_in
            div_in += 1
        if res_out and not (math.isnan(res_out)) and res_out != 0  :
            mean_mean_out += res_out
            div_out += 1

    mean_mean_out = mean_mean_out/div_out
    mean_mean_in = mean_mean_in/div_in



    col_feature_partition.insert_one({
        "partition": partition,
        "num_node": num_node,
        "num_internal_edge": num_edge,
        "num_txns": num_txns,
        "ath_outflow": ath_outflow,
        "ath_inflow": ath_inflow,
        "lowest_outflow": lowest_outflow,
        "lowest_inflow": lowest_inflow,
        # "mean_all_out_among_node": mean_all_out_among_node,
        # "mean_all_in_among_node": mean_all_in_among_node,
        "total_out": total_out,
        "total_in": total_in,
        "mean_mean_out": mean_mean_out,
        "mean_mean_in": mean_mean_in,
        "first_date_txn": first_date_txn,
        "last_date_txn": last_date_txn
    })

def phan_tich_sodinh_socanh_moipar():
    res = list(col_feature_partition.find())
    for item in res:
        par = item.get("partition")
        num_node = item.get("num_node")
        num_internal_edge = item.get("num_internal_edge")
        text = f'(PH {par},{num_node})'
        print(text)
    
    for item in res:
        par = item.get("partition")
        num_node = item.get("num_node")
        num_internal_edge = item.get("num_internal_edge")
        text = f'(PH {par},{num_internal_edge})'
        print(text)

def thong_ke_mota():
    res = col_feature_partition.find()
    for item in res:
        ath_outflow = math.floor(item.get("ath_outflow")/1000000)
        ath_inflow = math.floor(item.get("ath_inflow")/1000000)
        total_out = math.floor(item.get("total_out")/1000000)
        total_in = math.floor(item.get("total_in")/1000000)
        mean_mean_out = math.floor(item.get("mean_mean_out")/1000000)
        mean_mean_in = math.floor(item.get("mean_mean_in")/10000)/100
        print(f'{item.get("partition")} &{ath_outflow} & {ath_inflow} & {total_out} & {total_in} & {mean_mean_out} & {mean_mean_in}', end="")
        print("\\\\")
        print("\hline")

thong_ke_mota()





