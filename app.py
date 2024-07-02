import io
import sqlite3

import pandas as pd
import streamlit as st

from orderpush.models import Order

st.title("Market")


def get_data_symbol():
    conn = sqlite3.connect("database.db")
    data = pd.read_sql_query("SELECT * FROM symbol", conn)
    data = data.rename(
        columns={
            "symbol": "Symbol",
            "close": "Close",
            "limitUp": "Limit Up",
            "limitDown": "Limit Down",
            "open": "Open",
            "high": "High",
            "low": "Low",
            "lastPrice": "Last Price",
            "volume": "Volume",
        }
    )
    numeric_cols = [
        "Close",
        "Limit Up",
        "Limit Down",
        "Open",
        "High",
        "Low",
        "Last Price",
        "Volume",
    ]
    data[numeric_cols] = data[numeric_cols].map(lambda x: "{:.2f}".format(x))
    conn.close()
    return data


def get_pending_orders():
    conn = sqlite3.connect("database.db")
    data = pd.read_sql_query(
        "SELECT clOrdID, symbol, side, ordType, price, quantity, executedQuantity, lastExecutedQuantity, lastExecutedPrice, openQuantity FROM pending_order",
        conn,
    )
    data = data.rename(
        columns={
            "clOrdID": "Order ID",
            "symbol": "Symbol",
            "side": "Side",
            "ordType": "Order Type",
            "price": "Price",
            "quantity": "Quantity",
            "executedQuantity": "Executed Quantity",
            "lastExecutedQuantity": "Last Executed Quantity",
            "lastExecutedPrice": "Last Executed Price",
            "openQuantity": "Open Quantity",
        }
    )
    mumeric_cols = [
        "Price",
        "Quantity",
        "Executed Quantity",
        "Last Executed Quantity",
        "Last Executed Price",
        "Open Quantity",
    ]
    data[mumeric_cols] = data[mumeric_cols].map(lambda x: "{:.2f}".format(x))
    conn.close()
    return data


def get_order_history():
    conn = sqlite3.connect("database.db")
    data = pd.read_sql_query(
        "SELECT clOrdID, symbol, side, ordType, price, quantity, executedQuantity, lastExecutedQuantity, lastExecutedPrice FROM order_history",
        conn,
    )
    data = data.rename(
        columns={
            "clOrdID": "Order ID",
            "symbol": "Symbol",
            "side": "Side",
            "ordType": "Order Type",
            "price": "Price",
            "quantity": "Quantity",
            "executedQuantity": "Executed Quantity",
            "lastExecutedQuantity": "Last Executed Quantity",
            "lastExecutedPrice": "Last Executed Price",
        }
    )
    mumeric_cols = [
        "Price",
        "Quantity",
        "Executed Quantity",
        "Last Executed Quantity",
        "Last Executed Price",
    ]
    data[mumeric_cols] = data[mumeric_cols].map(lambda x: "{:.2f}".format(x))
    conn.close()
    return data


def find_symbol(symbol):
    conn = sqlite3.connect("database.db")
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM symbol WHERE symbol = ?", (symbol,))
    row = cursor.fetchone()
    conn.close()
    return row


def is_number(n):
    try:
        float(n)
        return True
    except ValueError:
        return False


def main(file: io.TextIOWrapper):
    clOrdID = 1
    # Start the Python script and create a pipe to communicate with it
    # process = subprocess.Popen(['python', 'ordermatch.py', 'ordermatch.cfg', 'settings.yaml'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE)
    # Display the script's output
    # while True:
    #     output = process.stdout.readline().decode('utf-8')
    #     if output == '' and process.poll() is not None:
    #         break
    #     print(output.strip())
    #     if '35=8' in output.strip() and '39=0' in output.strip():
    #         success_message = st.sidebar.empty()
    #         success_message.success('New order successfully placed!')
    #         time.sleep(5)
    #         success_message.empty()

    page = st.sidebar.selectbox("Choose a page", ["Home", "Orders"])
    if page == "Home":
        df = pd.DataFrame(get_data_symbol())
        st.table(df)
        with st.sidebar.form(key="my_form"):
            symbols = df["Symbol"].tolist()

            symbol = st.selectbox("Symbol", symbols)
            price = st.text_input(label="Price")
            quantity = st.text_input(label="Quantity")
            side = st.selectbox("Side", ["BUY", "SELL"])
            submit_button = st.form_submit_button(label="Submit")
            symbol_data = find_symbol(symbol)
            if submit_button:
                if symbol == "" or price == "" or quantity == "":
                    st.error("Error: All fields must be filled in.")
                elif not is_number(price) or not is_number(quantity):
                    st.error("Error: Price and Quantity must be numbers.")
                elif symbol_data is None:
                    st.error("Error: Symbol not found.")
                elif symbol_data[2] < float(price) or symbol_data[3] > float(price):
                    st.error("Error: Price is out of range.")
                else:
                    side_number = '1' if side == "BUY" else '2' if side == "SELL" else 0
                    print(side)
                    order = Order(str(clOrdID), symbol, price, side_number, quantity)
                    print(order)
                    file.write(order.to_csv() + "\n")
                    file.flush()
                    # application.sendNewOrderSingle(order)
                    # Increment the ClOrdID for next order
                    clOrdID += 1

    elif page == "Orders":
        order_page = st.sidebar.selectbox(
            "Order Page", ["Pending Orders", "Order History"], key="order_page"
        )
        if order_page == "Pending Orders":
            df = pd.DataFrame(get_pending_orders())
            st.table(df)
            action = st.sidebar.selectbox("Action", ["Cancel", "Modify"])
            if action == "Modify":
                with st.sidebar.form(key="modify_form"):
                    symbols = df["Symbol"].tolist()
                    order_ids = df["Order ID"].tolist()
                    selected_order_id = st.selectbox(
                        "Order ID",
                        order_ids,
                        format_func=lambda x: x if x else "No orders available",
                    )
                    if not order_ids:  # If the list is empty, disable the form
                        st.warning("No pending orders available to modify.")
                        st.stop()
                    order_id = df.loc[df["Order ID"] == selected_order_id].index[0]
                    new_symbol = st.selectbox("New symbol", symbols)
                    new_side = st.selectbox("New side", ["BUY", "SELL"])
                    new_type = st.selectbox("New type", ["MARKET", "LIMIT"])
                    new_price = st.number_input("New price")
                    new_quantity = st.number_input("New quantity")
                    if st.form_submit_button("Submit"):
                        st.write(
                            f"Order {order_id} has been modified to {new_symbol}, {new_side}, {new_type}, {new_price}, {new_quantity}"
                        )
            if action == "Cancel":
                with st.sidebar.form(key="order_form"):
                    order_ids = df["Order ID"].tolist()
                    selected_order_id = st.selectbox(
                        "Order ID",
                        order_ids,
                        format_func=lambda x: x if x else "No orders available",
                    )
                    if not order_ids:  # If the list is empty, disable the form
                        st.warning("No pending orders available to cancel.")
                        st.stop()
                    order_id = df.loc[df["Order ID"] == selected_order_id].index[0]
                    if st.form_submit_button("Submit"):
                        st.write(f"Order {order_id} has been canceled.")

        elif order_page == "Order History":
            df = pd.DataFrame(get_order_history())
            st.table(df)


if __name__ == "__main__":
    # configs = Configs(sys.argv[1])

    # id_generator = generate_id(configs.snowflake_node_id)
    # application = FixApplication(id_generator)
    # settings = qf.SessionSettings(configs.fix_config)
    # storefactory = qf.FileStoreFactory(settings)
    # logfactory = qf.FileLogFactory(settings)
    # initiator = qf.SocketInitiator(application, storefactory, settings, logfactory)
    # initiator.start()
    with open("order.csv", "a+") as f:
        main(f)
