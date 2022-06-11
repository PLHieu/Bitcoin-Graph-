Ý nghĩa các file:
    // no care
    graph/lable_edges.py: dùng để mark từ address string sang một nhãn int để giảm size lưu trữ cũng như process dữ liệu được tốt hơn, chỉ chạy 1 lần duy nhất

    // no care
    graph/extract_graph.py: 
        - extract_group: update trường group của bảng famous_address, group này là group theo exchange và country, chỉ chạy 1 lần để updateMany collection
        - map_graph: Extract các cạnh trong trường hợp group theo exchange và country
        - extract_edge: 
    
    graph/extract_graph_old_strategy.py:
        - push_process_old_strategy_queue: distinct qua tưng block của các txn đã extract rồi push vào queue  process_old_strategy_queue, tuy nhiên đã không sử dụng cách này vì quá chậm, do đó chuyển sang sử dụng cache vào ram + loop qua các block để có thể tối ưu được tốc độ
        - Loop qua từng block:
            + Extract danh sách txn của block đó
            + Loop qua từng txn
            + các add trong trong inputs sẽ cùng 1 group
            + mỗi add output là 1 group
            + cách object để cache lại trong RAM: 
                cache_map_add_label: cache lại add -> belong to which label . Đỡ phải query label của address
                map_add_group: add -> belong to which group
                map_group_addresses: group -> contains which addresses
        - Output là map_group_addresses: group được các address, tuy nhiên chỉ lưu label chứ không lưu string
    
    graph/edge_inside_a_exchange.py:
        - extract_edge_v2(start_block, end_block): extract các cạnh từ startblock đến endblock , sau đó lưu lại vào list_edges.json
            {"address_from": "1DX9i4GA9uJFuEu2RVX88voET8Pnq3yF2k", "label_from": 201422, "group_from": 1843134, "address_to": "1NLWrvUZmTT63qmxTXXjsQbmtvP9HErXG9", "label_to": 202348, "group_to": 1843134}
        - Sau đó đọc file list_edges.json rồi extract thành set_edges.csv (bởi vì trong list_edges.json có thể có các cạnh trung nhau)

        - 

    graph/analytics_with_networkx.py:
        - đọc file graph -> phân tích

    graph/analytics_with_snap.py:
        - đọc file graph -> phân tích

    graph/agg_inoutflow.py:
        - từ bảng famous_inoutflow_v2 (của tưng add) -> inoutflow theo ngày của từng group. Process theo queue, mỗi item là 1 address

    
Ý nghĩa các queue:
    - queue_agg_io: 
        "data" : {
            "_id" : ObjectId("6280b06038b28e10601a34b5"), 
            "address" : NumberInt(794907), 
            "group" : NumberInt(1843084)
        }, 
        Cư mỗi item sẽ vào bảng famous_inoutflow_v2 để tính agg_famous_inoutflow_v2
    
    - queue_aggregate_inoutflow: 

    // nocare
    - process_old_strategy_queue: không sử dụng cách này vì quá chậm, do đó chuyển sang sử dụng cache vào ram


Ý nghĩa các collection:
    - agg_famous_inoutflow_v2: inoutflow theo ngày của từng group

    - map_add_group: address nào thuộc group nào, output của extract_graph_old_strategy.py

    // nocare
    - edge: Chứa các cạnh của đồ thị, tuy nhiên đồ thị lúc này là mỗi đỉnh theo 1 địa chỉ ví. 2 ví xuẩ hiện trong input/output có txn với nhau thì thành 1 cạnh . Tuy nhiên chiến thuật bị bỏ vì có quá nhiều đỉnh và quá nhiều cạnh -> đã gom nhóm các address

    // nocare
    - map_group_group: map từ group nào đến group nào, cứ 2 group có chung cạnh thì 2 group đó có thể gom nhóm lại được , dùng để gom nhóm theo chiến thuật cũ (bfs)
    
    // nocare
    - group_index: để lấy group_id mới bằng toán tử $inc, dùng để gom nhóm theo chiến thuật cũ (bfs)

    // nocare
    - extracted_graph:Dùng để group theo exchange và country, mỗi cặp unique exchange + country sẽ là 1 group
        { 
        "_id" : ObjectId("626debfb8a40b857399a9e69"), 
        "country" : "USA", 
        "exchange" : "Bittrex.com", 
        "group" : NumberInt(0)
        }
        
    
    // nocare
    - map_graph: Dùng để lưu lại các cạnh trong trường hợp group theo exchange và country, thiết lập 1 cạnh nếu có cùng country hoặc cùng exchange
        { 
            "_id" : ObjectId("626e423e95be0966c4ccbf2e"), 
            "group_from" : NumberInt(0), 
            "group_to" : NumberInt(4), 
            "same_in_country" : "USA",
            "same_in_exchange" : "bitrex",
        }




Các bước tiếp tục công việc để extract data:





// Phương pháp hiện tại
Có quá nhiều đỉnh + rời rạc lẫn nhau cho nên:
    Sử dụng thuật toán sau để mark các group: 
        các add cung input thì gom lại thành 1 group
        môi output thì sẽ gom lại thành 1 group

    Sau đó gom nhóm các group lại với nhau:
        Cách làm trước đây: Sử dụng stack để gom nhóm các group và thêm các field check vào trong mongodb
        Cách làm hiện tại: Xài dictionary của python + cache lại bằng dictionary
    
    Sau khi có được có group của các address thì loop qua các txn (thật ra không phải là loop mà là xài queue để tăng tốc) -> thêm vào 1 cái set adges

    Lưu ra file 

    // xử lý thêm graph một lần cuối nữa
    Sủ dụng snap để tìm kiếm các thành phần liên thông -> lấy thành phần liên thông có nhiều đỉnh nhất 

    ==> đồ thị cuối cùng



1. Bật consumed queue block để nó tiếp tục được consumed -> đợi cho consumed hết


--------------------------------------------------------------------------------
chạy file extract_graph_old_strategy.py để nó gom nhóm các address lại với nhau
chạy file edge_inside_a_exchange.py để extract đồ thị giữa các group


