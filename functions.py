import os
import numpy as np
import pandas as pd
from datetime import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta


# ****************************************************************************************************

# ----------------------------------------------------------------------------------------------------
# Functions for preprocessing
# ----------------------------------------------------------------------------------------------------


def same_day_date_range(str_date="2020-12-01"):
    """
    This function helps turning string date formats into
    datetime objects.
    """
    start_time_obj = datetime.strptime(str_date, "%Y-%m-%d")
    duration_to_add = timedelta(hours=23, minutes=59)
    end_time_obj = start_time_obj + duration_to_add

    return start_time_obj, end_time_obj


# ========================================SEQUENCE==========================================


def sequence_handler(dataframe, sequence_column, min_val, max_val):
    data = dataframe[["createdDate", "fcnt", "sequence"]]
    data["createdDate"] = pd.to_datetime(data["createdDate"])
    data = data.sort_values("createdDate")

    df = data.copy()
    df = df.rename(columns={"createdDate": "Date"})

    df["Stamp"] = "found"

    # Identify duplicates only if they are next to each other
    mask = df[sequence_column] == df[sequence_column].shift(-1)

    # Keep the last occurrence
    df = df[~mask | mask.shift(-1).fillna(False)]

    # Container for subsequences dataframes
    df_subsequences = []

    # Picking the first row
    df_current_subsequence = df.iloc[[0]]

    # Look through the second row till the end
    for i in range(1, len(df)):
        # if the current sequence number is lesser than the preceding,
        if df[sequence_column].iloc[i] < df[sequence_column].iloc[i - 1]:
            # Start a new subsequence when the sequence number decreases
            df_subsequences.append(df_current_subsequence.copy())

            # Start a new current subsequence with the current element
            df_current_subsequence = df.iloc[[i]]
        else:
            # If the current element is not less than the precedent, append it to the current subsequence
            df_current_subsequence = pd.concat([df_current_subsequence, df.iloc[[i]]])

    # Adding the last subsequence to the list of subsequences
    df_subsequences.append(df_current_subsequence.copy())

    # Total subsequences in the df_subsequences
    len_df_subsequences = len(df_subsequences)

    # If there are more than 2 subsequences,
    if len_df_subsequences > 2:
        # Isolate first and the last subsequences for special treatment
        first_df_subsequences, last_df_subsequences = (
            df_subsequences[0],
            df_subsequences[-1],
        )

        # For the sequences in the middle, loop through the values and idx
        for idx, sub in enumerate(df_subsequences[1:-1]):
            # Create a DataFrame with all sequences from min_val to max_val
            all_sequences = pd.DataFrame(
                {sequence_column: np.arange(min_val, max_val + 1)}
            )

            # Merge the original DataFrame with the DataFrame containing all sequences
            merged_df = pd.merge(all_sequences, sub, on=sequence_column, how="left")

            # Mark the missing sequences as 'Unfound'
            #             merged_df["Stamp"] = merged_df["Stamp"].fillna("Unfound")

            # Reorder columns if needed
            merged_df = merged_df[["Date", sequence_column, "fcnt", "Stamp"]]

            print("==============================================")
            #             print("SUB-SEQUENCE # {}\n:{}".format(idx + 2, merged_df))
            print("==============================================")

            # Conver Date to datetime
            merged_df["Date"] = pd.to_datetime(merged_df["Date"])
            #             merged_df['Date'] = merged_df['Date'].interpolate()

            # Use ffill first to fill out null dates
            merged_df["Date"] = merged_df["Date"].fillna(method="ffill")

            # Use bill later to fill out the null dates
            merged_df["Date"] = merged_df["Date"].fillna(method="bfill")

            # Replace the corresponding dataframe
            df_subsequences[idx + 1] = merged_df

        # For the first subsequence, create a dataframe with all numbers, starting from min of subsequence to max val.
        all_sequences_for_first = pd.DataFrame(
            {
                sequence_column: np.arange(
                    first_df_subsequences[sequence_column].min(), max_val + 1
                )
            }
        )
        # Do the merger of subsequence data and the one that was just created in the previous lines
        merged_df_first = pd.merge(
            all_sequences_for_first,
            first_df_subsequences,
            on=sequence_column,
            how="left",
        )

        # Fill null values of Stamp with unfound
        #         merged_df_first["Stamp"] = merged_df_first["Stamp"].fillna("Unfound")

        # Set up columns
        merged_df_first = merged_df_first[["Date", sequence_column, "fcnt", "Stamp"]]

        # change Date dtype
        merged_df_first["Date"] = pd.to_datetime(merged_df_first["Date"])

        #         merged_df_first['Date'] = merged_df_first['Date'].interpolate()

        # Use ffill to fill out null dates
        merged_df_first["Date"] = merged_df_first["Date"].fillna(method="ffill")

        # Use bfill to fill out null dates
        merged_df_first["Date"] = merged_df_first["Date"].fillna(method="bfill")

        # Replace the corresponding subsequence
        df_subsequences[0] = merged_df_first

        # For the last subsequence, create a dataframe with all numbers, starting from min_val to the max of subsequence.
        all_sequences_for_last = pd.DataFrame(
            {
                sequence_column: np.arange(
                    min_val, last_df_subsequences[sequence_column].max() + 1
                )
            }
        )

        # Do the merger of subsequence data and the one that was just created in the previous lines
        merged_df_last = pd.merge(
            all_sequences_for_last, last_df_subsequences, on=sequence_column, how="left"
        )

        # Fill null values of Stamp with unfound
        #         merged_df_last["Stamp"] = merged_df_last["Stamp"].fillna("Unfound")

        # Set up columns
        merged_df_last = merged_df_last[["Date", sequence_column, "fcnt", "Stamp"]]

        print("==============================================")

        # change Date dtype
        merged_df_last["Date"] = pd.to_datetime(merged_df_last["Date"])

        #         merged_df_last['Date'] = merged_df_last['Date'].interpolate()

        # Use ffill to fill out null dates
        merged_df_last["Date"] = merged_df_last["Date"].fillna(method="ffill")

        # Use bfill to fill out null dates
        merged_df_last["Date"] = merged_df_last["Date"].fillna(method="bfill")

        # Replace the corresponding subsequence
        df_subsequences[-1] = merged_df_last

    # If there are 2 subsequences,
    elif len_df_subsequences == 2:
        # Isolate first and the last subsequences for special treatment
        first_df_subsequences, last_df_subsequences = (
            df_subsequences[0],
            df_subsequences[-1],
        )

        # For the first subsequence, create a dataframe with all numbers, starting from min of subsequence to max val.
        all_sequences_for_first = pd.DataFrame(
            {
                sequence_column: np.arange(
                    first_df_subsequences[sequence_column].min(), max_val + 1
                )
            }
        )

        # Do the merger of subsequence data and the one that was just created in the previous lines
        merged_df_first = pd.merge(
            all_sequences_for_first,
            first_df_subsequences,
            on=sequence_column,
            how="left",
        )

        # Fill null values of Stamp with unfound
        #         merged_df_first["Stamp"] = merged_df_first["Stamp"].fillna("Unfound")

        # Set up columns
        merged_df_first = merged_df_first[["Date", sequence_column, "fcnt", "Stamp"]]

        # Convert Date to datetime
        merged_df_first["Date"] = pd.to_datetime(merged_df_first["Date"])

        # Use ffill to fill out null dates
        merged_df_first["Date"] = merged_df_first["Date"].fillna(method="ffill")

        # Use bfill to fill out null dates
        merged_df_first["Date"] = merged_df_first["Date"].fillna(method="bfill")

        #         merged_df_first['Date'] = merged_df_first['Date'].interpolate()

        # Replace with corresponding subsequence
        df_subsequences[0] = merged_df_first

        # For the last subsequence, create a dataframe with all numbers, starting from min_val to max of subsequence.
        all_sequences_for_last = pd.DataFrame(
            {
                sequence_column: np.arange(
                    min_val, last_df_subsequences[sequence_column].max() + 1
                )
            }
        )

        # Do the merger of subsequence data and the one that was just created in the previous lines
        merged_df_last = pd.merge(
            all_sequences_for_last, last_df_subsequences, on=sequence_column, how="left"
        )

        # Fill null values of Stamp with unfound
        #         merged_df_last["Stamp"] = merged_df_last["Stamp"].fillna("Unfound")

        # Setup columns
        merged_df_last = merged_df_last[["Date", sequence_column, "fcnt", "Stamp"]]
        print("==============================================")

        # Convert Date to datetime
        merged_df_last["Date"] = pd.to_datetime(merged_df_last["Date"])
        #         merged_df_last['Date'] = merged_df_last['Date'].interpolate()

        # Use ffill to fill out null dates
        merged_df_last["Date"] = merged_df_last["Date"].fillna(method="ffill")

        # Use bfill to fill out null dates
        merged_df_last["Date"] = merged_df_last["Date"].fillna(method="bfill")

        # Replace with corresponding subsequence
        df_subsequences[-1] = merged_df_last

    # If there is only 1 subsequences,
    elif len_df_subsequences == 1:
        # Isolate it
        only_df_subsequences = df_subsequences[0]

        # For this only subsequence, create a dataframe with all numbers, starting from min of subsequence to max of the subsequence.
        all_sequences_for_only = pd.DataFrame(
            {
                sequence_column: np.arange(
                    only_df_subsequences[sequence_column].min(),
                    only_df_subsequences[sequence_column].max() + 1,
                )
            }
        )

        # Do the merger of subsequence data and the one that was just created in the previous lines
        merged_df_only = pd.merge(
            all_sequences_for_only, only_df_subsequences, on=sequence_column, how="left"
        )

        # Fill null values of Stamp with unfound
        #         merged_df_only["Stamp"] = merged_df_only["Stamp"].fillna("Unfound")

        # Set up columns
        merged_df_only = merged_df_only[["Date", sequence_column, "fcnt", "Stamp"]]
        print("==============================================")
        # print(merged_df_only)

        # Convert Date to datetime
        merged_df_only["Date"] = pd.to_datetime(merged_df_only["Date"])

        # Use ffill to fill out null dates
        merged_df_only["Date"] = merged_df_only["Date"].fillna(method="ffill")

        # Use bfill to fill out null dates
        merged_df_only["Date"] = merged_df_only["Date"].fillna(method="bfill")

        #         merged_df_only['Date'] = merged_df_only['Date'].interpolate()

        # Replace with corresponding subsequence
        df_subsequences[0] = merged_df_only

    # Concatenate the reulsts
    df_sequence = pd.concat(df_subsequences, ignore_index=True).reset_index()

    difference = []

    find_diff_df = df_sequence.dropna(subset=["fcnt"])

    seq_list_diff = find_diff_df["sequence"].to_list()

    for i in range(len(seq_list_diff) - 1):
        current_seq = seq_list_diff[i]
        next_seq = seq_list_diff[i + 1]
        diff = next_seq - current_seq

        if diff == 1:
            difference.append(0)
        elif diff == 2:
            difference.append(1)
        elif diff > 1:
            difference.append(diff - 1)
        elif diff == 0:
            difference.append(diff)
        elif diff < 0:
            val_1 = 255 - current_seq
            val_2 = next_seq
            diff = val_1 + val_2
            difference.append(diff)

    placeholder_value = 0
    if len(difference) < len(find_diff_df):
        difference.append(placeholder_value)

    find_diff_df["difference_column"] = difference
    merged = pd.merge(
        df_sequence,
        find_diff_df[["difference_column"]],
        left_index=True,
        right_index=True,
        how="left",
    )

    merged["difference_column"] = merged["difference_column"].where(
        merged["difference_column"].notnull(), None
    )

    stamp_df = merged.dropna(subset=["fcnt"])

    seq = stamp_df["sequence"].to_list()
    fcnt_lis = stamp_df["fcnt"].to_list()
    difference = stamp_df["difference_column"].to_list()

    my_list = []
    for i in range(len(difference) - 1):
        current_seq = seq[i]
        next_seq = seq[i + 1]
        diff = next_seq - current_seq

        if difference[i] >= 1:
            if diff < 0:
                if fcnt_lis[i] > fcnt_lis[i + 1]:
                    my_list.extend(["found"] * int(difference[i]))
                else:
                    my_list.extend(["Unfound"] * int(difference[i]))
            else:
                my_list.extend(["Unfound"] * int(difference[i]))

    condition = merged["Stamp"].isnull()
    merged.loc[condition, "Stamp"] = my_list[: condition.sum()]

    merged = merged[["index", "Date", "sequence", "Stamp"]]

    return merged


# ========================================OCCUPANCY==========================================


def calculate_occupancy_durations(dataframe):
    dataframe["createdDate"] = pd.to_datetime(
        dataframe["createdDate"]
    )  # Convert to datetime
    dataframe.sort_values(by="createdDate", inplace=True)
    dataframe.reset_index(drop=True, inplace=True)
    duration_dict = {}
    last_occupied_date = None

    for index, row in dataframe.iterrows():
        occupancy_status = row["occupancy_status"]
        created_date = row["createdDate"]
        message_log_time = int(
            created_date.timestamp() * 1000
        )  # Convert to milliseconds

        if occupancy_status == "occupied":
            if last_occupied_date is None:
                last_occupied_date = created_date
        elif last_occupied_date is not None:
            current_time_milli = int(datetime.now().timestamp() * 1000)
            duration_minutes = (
                message_log_time - last_occupied_date.timestamp() * 1000
            ) / 60000
            duration_dict[last_occupied_date] = duration_minutes
            last_occupied_date = None

    # If the last entry is occupied, calculate its duration until the current time
    if last_occupied_date is not None:
        current_time_milli = int(datetime.now().timestamp() * 1000)
        duration_minutes = (
            current_time_milli - last_occupied_date.timestamp() * 1000
        ) / 60000
        duration_dict[last_occupied_date] = duration_minutes

    return duration_dict


# ========================================PARSING PAYLOAD==========================================


def status(row):
    if row[0] in ["0", "1", "2", "3", "4", "5", "6", "7"]:
        return "unoccupied"
    elif row[0].lower() in ["8", "9", "a", "b", "c", "d", "e", "f"]:
        return "occupied"


def frame_type(row):
    if row[1] == "0":
        return "info"
    elif row[1] == "1":
        return "keep-alive"
    elif row[1] == "2":
        return "Configuration Uplink"
    elif row[1] == "4":
        return "Start frame 1"
    elif row[1] == "5":
        return "Start frame 2"
    elif row[1] == "7":
        return "RTC Update"


def parse_sequence(row, base=16):
    """
    Get Sequence from Payload
    """
    hex_val = row[2:4]
    int_val = int(hex_val, base)
    seq_val = int_val
    return seq_val


def validate_voltage(row, base=16):
    """
    Get battery from Payload
    """
    valid_c2 = ["7", "1", "4"]
    frame_value = str(row)[1]
    if frame_value in ["1", "7"]:  # Keep-Alive and RTC
        hex_val = row[-2:]
        int_val = int(hex_val, base)
        bat_val = int_val * 4 + 2800
        return bat_val
    elif frame_value == "4":
        hex_val = row[6:8]  # Start Frame
        int_val = int(hex_val, base)
        bat_val = int_val * 4 + 2800
        return bat_val
    else:
        bat_val = np.nan
        return bat_val


def validate_temperature(row, base=16):
    """
    Get voltage from temperature
    """
    hex_val = row[6:8]
    int_val = int(hex_val, base)
    temp_val = int_val
    return temp_val


def date_range(start_date="2023-07-09", end_date="2023-07-10", selector_value=""):
    """
    This function helps turning string date formats into
    datetime objects.
    """
    # Convert the input date string to a datetime object
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    if selector_value == "twentyfourhours":
        start_date = start_date
    elif selector_value == "weekly":
        start_date = start_date - relativedelta(days=7)
    elif selector_value == "fortnightly":
        start_date = start_date - relativedelta(days=15)
    elif selector_value == "monthly":
        # Calculate 1 month before using relativedelta, which takes care of variable month lengths
        start_date = start_date - relativedelta(months=1)
    elif selector_value == "3_month":
        # Calculate 1 month before using relativedelta, which takes care of variable month lengths
        start_date = start_date - relativedelta(months=3)

    # Format the results as strings in the 'YYYY-MM-DD' format
    start_date = start_date.strftime("%Y-%m-%d")
    start_time_str = start_date + " 00:00:00"
    start_time_obj = datetime.strptime(start_time_str, "%Y-%m-%d %H:%M:%S")

    # Adjust the end_time_obj to set time to 23:59:59
    end_time_str = end_date + " 23:59:59"
    end_time_obj = datetime.strptime(end_time_str, "%Y-%m-%d %H:%M:%S")

    return start_time_obj, end_time_obj


def parse_header(row):
    bit_00 = str(row)[0]
    #     print(bit_000)
    bit_01 = str(row)[1]
    #     print(bit_001)
    dict_bit_01 = dict()

    if bit_00 == "0":
        dict_bit_01["occupancy_status"] = "Unoccupied"
        dict_bit_01["battery_state"] = "Good Battery"
        dict_bit_01["acknowledgment"] = "Acknowledged"
        dict_bit_01["radar calibration"] = "No radar calibration"
    elif bit_00 == "1":
        dict_bit_01["occupancy_status"] = "Unoccupied"
        dict_bit_01["battery_state"] = "Good Battery"
        dict_bit_01["acknowledgment"] = "Acknowledged"
        dict_bit_01[
            "radar calibration"
        ] = "Recalibration was done since last successful uplink"
    elif bit_00 == "2":
        dict_bit_01["occupancy_status"] = "Unoccupied"
        dict_bit_01["battery_state"] = "Good Battery"
        dict_bit_01["acknowledgment"] = "Not Acknowledged"
        dict_bit_01["radar calibration"] = "No radar calibration"
    elif bit_00 == "3":
        dict_bit_01["occupancy_status"] = "Unoccupied"
        dict_bit_01["battery_state"] = "Good Battery"
        dict_bit_01["acknowledgment"] = "Not Acknowledged"
        dict_bit_01[
            "radar calibration"
        ] = "Recalibration was done since last successful uplink"
    elif bit_00 == "4":
        dict_bit_01["occupancy_status"] = "Unoccupied"
        dict_bit_01["battery_state"] = "Low Battery"
        dict_bit_01["acknowledgment"] = "Acknowledged"
        dict_bit_01["radar calibration"] = "No radar calibration"
    elif bit_00 == "5":
        dict_bit_01["occupancy_status"] = "Unoccupied"
        dict_bit_01["battery_state"] = "Low Battery"
        dict_bit_01["acknowledgment"] = "Acknowledged"
        dict_bit_01[
            "radar calibration"
        ] = "Recalibration was done since last successful uplink"
    elif bit_00 == "6":
        dict_bit_01["occupancy_status"] = "Unoccupied"
        dict_bit_01["battery_state"] = "Low Battery"
        dict_bit_01["acknowledgment"] = "Not Acknowledged"
        dict_bit_01["radar calibration"] = "No radar calibration"
    elif bit_00 == "7":
        dict_bit_01["occupancy_status"] = "Unoccupied"
        dict_bit_01["battery_state"] = "Low Battery"
        dict_bit_01["acknowledgment"] = "Not Acknowledged"
        dict_bit_01[
            "radar calibration"
        ] = "Recalibration was done since last successful uplink"
    elif bit_00 == "8":
        dict_bit_01["occupancy_status"] = "Occupied"
        dict_bit_01["battery_state"] = "Good Battery"
        dict_bit_01["acknowledgment"] = "Acknowledged"
        dict_bit_01["radar calibration"] = "No radar calibration"
    elif bit_00 == "9":
        dict_bit_01["occupancy_status"] = "Occupied"
        dict_bit_01["battery_state"] = "Good Battery"
        dict_bit_01["acknowledgment"] = "Acknowledged"
        dict_bit_01[
            "radar calibration"
        ] = "Recalibration was done since last successful uplink"
    elif bit_00 == "a":
        dict_bit_01["occupancy_status"] = "Occupied"
        dict_bit_01["battery_state"] = "Good Battery"
        dict_bit_01["acknowledgment"] = "Not Acknowledged"
        dict_bit_01["radar calibration"] = "No radar calibration"
    elif bit_00 == "b":
        dict_bit_01["occupancy_status"] = "Occupied"
        dict_bit_01["battery_state"] = "Good Battery"
        dict_bit_01["acknowledgment"] = "Not Acknowledged"
        dict_bit_01[
            "radar calibration"
        ] = "Recalibration was done since last successful uplink"
    elif bit_00 == "c":
        dict_bit_01["occupancy_status"] = "Occupied"
        dict_bit_01["battery_state"] = "Low Battery"
        dict_bit_01["acknowledgment"] = "Acknowledged"
        dict_bit_01["radar calibration"] = "No radar calibration"
    elif bit_00 == "d":
        dict_bit_01["occupancy_status"] = "Occupied"
        dict_bit_01["battery_state"] = "Low Battery"
        dict_bit_01["acknowledgment"] = "Acknowledged"
        dict_bit_01[
            "radar calibration"
        ] = "Recalibration was done since last successful uplink"
    elif bit_00 == "e":
        dict_bit_01["occupancy_status"] = "Occupied"
        dict_bit_01["battery_state"] = "Low Battery"
        dict_bit_01["acknowledgment"] = "Not Acknowledged"
        dict_bit_01["radar calibration"] = "No radar calibration"
    elif bit_00 == "f":
        dict_bit_01["occupancy_status"] = "Occupied"
        dict_bit_01["battery_state"] = "Low Battery"
        dict_bit_01["acknowledgment"] = "Acknowledged"
        dict_bit_01[
            "radar calibration"
        ] = "Recalibration was done since last successful uplink"

    if bit_01 == "0":
        dict_bit_01["Frame_Type"] = "Info Frame"
    elif bit_01 == "1":
        dict_bit_01["Frame_Type"] = "Keep-alive frame"
    elif bit_01 == "2":
        dict_bit_01["Frame_Type"] = "Configuration uplink"
    elif bit_01 == "4":
        dict_bit_01["Frame_Type"] = "Start frame 1"
    elif bit_01 == "5":
        dict_bit_01["Frame_Type"] = "Start frame 2"
    elif bit_01 == "7":
        dict_bit_01["Frame_Type"] = "RTC update"
    return dict_bit_01


def parse_startframe_1(row, base=16):
    bit_val = str(row)[1]

    hex_val_seq = row[2:4]  # For sequence
    seq_val = int(hex_val_seq, base)

    hex_val_fw = row[4:6]  # For Firmware value
    fw_val = int(hex_val_fw, base)

    if fw_val == 31:
        fw_val_version = "v1.1.1"
    elif fw_val == 32:
        fw_val_version = "v1.1.2"
    elif fw_val == 33:
        fw_val_version = "v1.1.3"
    elif fw_val == 34:
        fw_val_version = "v1.1.4"
    else:
        fw_val_version = "None"

    hex_val_bat = row[6:8]  # For Battery
    int_val_bat = int(hex_val_bat, base)
    bat_val = int_val_bat * 4 + 2800

    hex_val_radar_thres = row[8:10]  # For Radar threshold value
    int_radar_thres_val = int(hex_val_radar_thres, base)
    radar_thres_val = int_radar_thres_val * 100

    hex_val_rrs = row[10:12]  # For Radar range start
    rrs_val = int(hex_val_rrs, base)

    hex_val_rrl = row[12:14]  # For Radar range length
    int_rrl_val = int(hex_val_rrl, base)
    rrl_val = int_rrl_val + 20

    hex_val_ljm = row[14:16]  # For LoRaWAN join mode
    ljm_val = int(hex_val_ljm, base)

    hex_val_L_ADR = row[16:18]  # For LoRaWAN ADR
    L_ADR_val = int(hex_val_L_ADR, base)

    hex_val_rsb = row[18:20]  # For Reset source bitmap
    rsb_val = int(hex_val_rsb, base)

    dict_bit_startframe_1 = dict()
    if bit_val == "4":
        dict_bit_startframe_1["Frame Type"] = "Start frame 1"
        dict_bit_startframe_1["Sequence Number"] = seq_val
        dict_bit_startframe_1["Firmware Version"] = fw_val_version
        dict_bit_startframe_1["Battery"] = bat_val
        dict_bit_startframe_1["Radar threshold"] = radar_thres_val
        dict_bit_startframe_1["Radar range start"] = rrs_val
        dict_bit_startframe_1["Radar range length"] = rrl_val
        dict_bit_startframe_1["LoRaWAN join mode"] = ljm_val
        dict_bit_startframe_1["LoRaWAN ADR"] = L_ADR_val
        dict_bit_startframe_1["Reset source bitmap"] = rsb_val

    return dict_bit_startframe_1


def parse_startframe_2(row, base=16):
    bit_val = str(row)[1]

    hex_val_seq = row[2:4]  # For sequence
    seq_val = int(hex_val_seq, base)

    hex_val_sleep_time_min = row[4:6]  # For Sleep time minutes value
    sleep_time_min_val = int(hex_val_sleep_time_min, base)

    hex_val_sleep_time_sec = row[6:8]  # For Sleep time seconds value
    sleep_time_sec_val = int(hex_val_sleep_time_sec, base)

    hex_val_keep_alive = row[8:10]  # For Keep Alive value
    keep_alive_val = int(hex_val_keep_alive, base)

    hex_val_night_mode = row[10:12]  # For Night mode value
    night_mode_val = int(hex_val_night_mode, base)

    hex_val_night_mode_start_hr = row[12:14]  # For Night mode start hour
    night_mode_start_hr_val = int(hex_val_night_mode_start_hr, base)

    hex_val_night_mode_duration = row[14:16]  # For Night mode duration
    night_mode_duration_val = int(hex_val_night_mode_duration, base)

    hex_val_night_mode_sleep = row[16:18]  # For Night mode sleep time
    night_mode_sleep_val = int(hex_val_night_mode_sleep, base)

    hex_val_night_mode_alive = row[18:20]  # For Night mode keep alive
    night_mode_alive_val = int(hex_val_night_mode_alive, base)

    dict_bit_startframe_2 = dict()
    if bit_val == "5":
        dict_bit_startframe_2["Frame Type"] = "Start frame 2"
        dict_bit_startframe_2["Sequence Number"] = seq_val
        dict_bit_startframe_2["Sleep time minutes"] = sleep_time_min_val
        dict_bit_startframe_2["Sleep time seconds"] = sleep_time_sec_val
        dict_bit_startframe_2["Keep-alive"] = keep_alive_val
        dict_bit_startframe_2["Night-mode"] = night_mode_val
        dict_bit_startframe_2["Night-mode start hour"] = night_mode_start_hr_val
        dict_bit_startframe_2["Night-mode duration"] = night_mode_duration_val
        dict_bit_startframe_2["Night-mode sleep time"] = night_mode_sleep_val
        dict_bit_startframe_2["Night-mode keep-alive"] = night_mode_alive_val

    return dict_bit_startframe_2


def parse_infoframe(row, base=16):
    bit_val = str(row)[1]

    hex_val_seq = row[2:4]  # For sequence
    seq_val = int(hex_val_seq, base)

    hex_val_radar = row[4:6]  # For Radar error
    radar_val = int(hex_val_radar, base)

    hex_val_temp = row[6:8]  # For temperature
    temp_val = int(hex_val_temp, base)

    hex_val_time_hh = row[8:10]  # For timestamp (hh)
    time_hh_val = int(hex_val_time_hh, base)

    hex_val_time_mm = row[10:12]  # For timestamp (mm)
    time_mm_val = int(hex_val_time_mm, base)

    dict_infoframe = dict()
    if bit_val == "0":
        dict_infoframe["Frame Type"] = "Info Frame"
        dict_infoframe["Sequence number "] = seq_val
        dict_infoframe["Radar error"] = radar_val
        dict_infoframe["Temperature"] = temp_val
        dict_infoframe["Timestamp (hh)"] = time_hh_val
        dict_infoframe["Timestamp (mm)"] = time_mm_val

    return dict_infoframe


def parse_keepalive(row, base=16):
    bit_val = str(row)[1]

    hex_val_seq = row[2:4]  # For sequence
    seq_val = int(hex_val_seq, base)

    hex_val_radar = row[4:6]  # For Radar error
    radar_val = int(hex_val_radar, base)

    hex_val_temp = row[6:8]  # For temperature
    temp_val = int(hex_val_temp, base)

    hex_val_time_hh = row[8:10]  # For timestamp (hh)
    time_hh_val = int(hex_val_time_hh, base)

    hex_val_time_mm = row[10:12]  # For timestamp (mm)
    time_mm_val = int(hex_val_time_mm, base)

    hex_val_bat = row[-2:]  # For battery
    int_val_bat = int(hex_val_bat, base)
    bat_val = int_val_bat * 4 + 2800

    dict_keepalive = dict()
    if bit_val == "1":
        dict_keepalive["Frame Type"] = "Keep-alive frame"
        dict_keepalive["Sequence number "] = seq_val
        dict_keepalive["Radar error"] = radar_val
        dict_keepalive["Temperature"] = temp_val
        dict_keepalive["Timestamp (hh)"] = time_hh_val
        dict_keepalive["Timestamp (mm)"] = time_mm_val
        dict_keepalive["Battery"] = bat_val

    return dict_keepalive


def parse_rtc_update(row, base=16):
    bit_val = str(row)[1]

    hex_val_seq = row[2:4]  # For sequence
    seq_val = int(hex_val_seq, base)

    hex_val_radar = row[4:6]  # For Radar error
    radar_val = int(hex_val_radar, base)

    hex_val_temp = row[6:8]  # For temperature
    temp_val = int(hex_val_temp, base)

    hex_val_time_hh = row[8:10]  # For timestamp (hh)
    time_hh_val = int(hex_val_time_hh, base)

    hex_val_time_mm = row[10:12]  # For timestamp (mm)
    time_mm_val = int(hex_val_time_mm, base)

    hex_val_bat = row[-2:]  # For battery
    int_val_bat = int(hex_val_bat, base)
    bat_val = int_val_bat * 4 + 2800

    dict_rtc_update = dict()
    if bit_val == "7":
        dict_rtc_update["Frame Type"] = "RTC update request"
        dict_rtc_update["Sequence number "] = seq_val
        dict_rtc_update["Radar error"] = radar_val
        dict_rtc_update["Temperature"] = temp_val
        dict_rtc_update["Timestamp (hh)"] = time_hh_val
        dict_rtc_update["Timestamp (mm)"] = time_mm_val
        dict_rtc_update["Battery"] = bat_val

    return dict_rtc_update


def main_function(row):
    # Call individual functions
    result1 = parse_header(row)
    result2 = parse_startframe_1(row)
    result3 = parse_startframe_2(row)
    result4 = parse_infoframe(row)
    result5 = parse_keepalive(row)
    result6 = parse_rtc_update(row)

    # Combine results
    combined_results = {
        "Header Parsing": result1,
        "Startframe 1 Parsing": result2,
        "Startframe 2 Parsing": result3,
        "Info frame Parsing": result4,
        "Keep-alive frame Parsing": result5,
        "RTC update Parsing": result6,
    }

    return combined_results


# ========================================ABSENTEES & REAWAKEN==========================================


def grab_reports_for_comparison(input_date, DIR_daily_reports):
    all_files = os.listdir(DIR_daily_reports)
    # Filter files that match the naming pattern
    report_files = [
        file
        for file in all_files
        if file.startswith("Daily Report for ") and file.endswith(".xlsx")
    ]

    # Extract dates from the report names and convert to datetime objects
    report_dates = [
        datetime.strptime(report.split(" ")[3], "%Y-%m-%d") for report in report_files
    ]
    str_report_dates = [date.strftime("%Y-%m-%d") for date in report_dates]
    # Convert input_date to datetime object
    input_datetime = datetime.strptime(input_date, "%Y-%m-%d")
    # Convert each date in list_of_dates to datetime objects
    date_objects = [datetime.strptime(date, "%Y-%m-%d") for date in str_report_dates]
    # Find the last date before the input date in the list
    last_date = max(date for date in date_objects if date < input_datetime)

    # Convert last_date to string in the original format
    last_date_str = last_date.strftime("%Y-%m-%d")
    name_report_yesterday = next(
        (date for date in report_files if last_date_str in date), None
    )
    name_report_today = next(
        (date for date in report_files if input_date in date), None
    )

    report_yesterday = pd.read_excel(
        DIR_daily_reports + "/" + name_report_yesterday, sheet_name="All Sensors"
    )
    report_today = pd.read_excel(
        DIR_daily_reports + "/" + name_report_today, sheet_name="All Sensors"
    )

    report_yesterday["EUI"] = report_yesterday["EUI"].str.strip()
    report_today["EUI"] = report_today["EUI"].str.strip()

    # Yesterday's awake EUIs
    abs0_repy = (
        report_yesterday[
            report_yesterday["Days since sensor sent its Last message (Days)"] == "0"
        ]["EUI"]
        .str.strip()
        .tolist()
    )
    # Yesterday's 1 day absentees
    abs1_repy = (
        report_yesterday[
            report_yesterday["Days since sensor sent its Last message (Days)"] == "1"
        ]["EUI"]
        .str.strip()
        .tolist()
    )

    # Today's awake EUIs
    abs0_rept = (
        report_today[
            report_today["Days since sensor sent its Last message (Days)"] == "0"
        ]["EUI"]
        .str.strip()
        .tolist()
    )

    # Today's 1 day absentees
    abs1_rept = (
        report_today[
            report_today["Days since sensor sent its Last message (Days)"] == "1"
        ]["EUI"]
        .str.strip()
        .tolist()
    )

    # Comparison: New 1 day absentees
    sleepies_eui = [eui for eui in abs1_rept if eui in abs0_repy]
    new_sleepies = len(sleepies_eui)

    # Comparison: EUIs that woke up
    woken_up_eui = [eui for eui in abs0_rept if eui in abs1_repy]
    new_woken_up = len(woken_up_eui)

    return new_sleepies, new_woken_up, sleepies_eui, woken_up_eui


# ****************************************************************************************************

# ----------------------------------------------------------------------------------------------------
# Functions for data extractions through Mongo Pipelines
# ----------------------------------------------------------------------------------------------------


def get_start_end_date_active(conn, eui, date, selector_value, database, collection):
    """
    Pipeline credits: Hira Ahmed
    """

    if selector_value == "twentyfourhours":
        mili_sec_in_days = 86400000  # // 86400000 milliseconds in a day
    elif selector_value == "weekly":
        mili_sec_in_days = 86400000 * 7  # // 86400000 milliseconds in 7 day
    elif selector_value == "fortnightly":
        mili_sec_in_days = 86400000 * 15  # // 86400000 milliseconds in 15 day
    elif selector_value == "monthly":
        # # Convert the input date string to a datetime object
        # Format the datetime object as a string in the desired format
        temp_given_date = date.strftime("%Y-%m-%d")
        temp_given_date = datetime.strptime(temp_given_date, "%Y-%m-%d")
        # Calculate 1 month before using relativedelta, which takes care of variable month lengths
        one_month_before = temp_given_date - relativedelta(months=1)
        # # Determine the number of days in that month
        days_in_previous_month = (temp_given_date - one_month_before).days
        # print("days before one month......", str(int(days_in_previous_month)))
        # Calculate 1 month before using relativedelta, which takes care of variable month lengths
        mili_sec_in_days = 86400000 * int(
            days_in_previous_month
        )  # // 86400000 milliseconds in one month
    elif selector_value == "3_month":
        # # Convert the input date string to a datetime object
        # Format the datetime object as a string in the desired format
        temp_given_date = date.strftime("%Y-%m-%d")
        temp_given_date = datetime.strptime(temp_given_date, "%Y-%m-%d")
        # Calculate 1 month before using relativedelta, which takes care of variable month lengths
        three_month_before = temp_given_date - relativedelta(months=3)
        # # Determine the number of days in that month
        days_in_previous_month = (temp_given_date - three_month_before).days
        # print("days before one month......", str(int(days_in_previous_month)))
        # Calculate 1 month before using relativedelta, which takes care of variable month lengths
        mili_sec_in_days = 86400000 * int(
            days_in_previous_month
        )  # // 86400000 milliseconds in one month
    db = conn[database]
    collection = db[collection]
    # print()
    pipeline = [
        {"$match": {"cmd": "rx", "eui": eui, "createdDate": {"$lte": date}}},
        {"$group": {"_id": "null", "maxDate": {"$max": "$createdDate"}}},
        {
            "$project": {
                "start_date": {
                    "$subtract": [
                        "$maxDate",
                        mili_sec_in_days,
                    ]  # // 86400000 milliseconds in a day
                },
                "end_date": "$maxDate",
            }
        },
    ]
    try:
        df = pd.DataFrame(list(collection.aggregate(pipeline, allowDiskUse=True)))
    except:
        print("Cannot fetch the dates from active db...")

    try:
        df.drop("_id", axis=1, inplace=True)
    except:
        print("couln't find _id try with query check first")

    if database == "conurets_smart_park_st_pete":
        archive_flag = False
        if df.empty:
            print("Not found.............")
            df = get_start_end_date_archive(conn, eui, date, mili_sec_in_days)
            archive_flag = True
        else:
            archive_flag = False
        print("retrieved collection successfully!!!")

        return df, archive_flag
    else:
        return df


def get_start_end_date_archive(conn, eui, date, mili_sec_in_days):
    """
    Pipeline credits: Hira Ahmed
    """
    db = conn.conurets_smart_park_st_pete_archive
    collection = db.cd_device_data_log_archive
    pipeline = [
        {"$match": {"cmd": "rx", "eui": eui, "createdDate": {"$lte": date}}},
        {"$group": {"_id": "null", "maxDate": {"$max": "$createdDate"}}},
        {
            "$project": {
                "start_date": {
                    "$subtract": [
                        "$maxDate",
                        mili_sec_in_days,
                    ]  # // 86400000 milliseconds in a day
                },
                "end_date": "$maxDate",
            }
        },
    ]
    try:
        df = pd.DataFrame(list(collection.aggregate(pipeline, allowDiskUse=True)))
    except:
        print("Cannot fetch the dates from archieve db...")
    try:
        df.drop("_id", axis=1, inplace=True)
    except:
        print("couln't find _id try with query check first")

    print("retrieved collection successfully!!!")

    return df


def get_the_rssi_before_absentees_active(conn, eui, start_date, end_date, database, collection):
    """
    Pipeline credits: Hira Ahmed
    """
    db = conn[database]
    collection = db[collection]

    pipeline = [
        {
            "$match": {
                "cmd": "rx",
                "eui": eui,
                "createdDate": {"$gte": start_date, "$lte": end_date},
            }
        },
        {"$sort": {"createdDate": -1}},
        {
            "$project": {
                "eui": "$eui",
                "createdDate": "$createdDate",
                "rssi": "$rssi",
                "snr": "$snr",
                "fcnt": "$fcnt",
                "data": "$data",
            }
        },
    ]
    try:
        df = pd.DataFrame(list(collection.aggregate(pipeline, allowDiskUse=True)))
    except:
        print("Cannot fetch the rssi etc from active db...")

    return df


def get_the_rssi_before_absentees_archive(conn, eui, start_date, end_date):
    """
    Pipeline credits: Hira Ahmed
    """
    db = conn.conurets_smart_park_st_pete_archive
    collection = db.cd_device_data_log_archive

    pipeline = [
        {
            "$match": {
                "cmd": "rx",
                "eui": eui,
                "createdDate": {"$gte": start_date, "$lte": end_date},
            }
        },
        {"$sort": {"createdDate": -1}},
        {
            "$project": {
                "eui": "$eui",
                "createdDate": "$createdDate",
                "rssi": "$rssi",
                "snr": "$snr",
                "fcnt": "$fcnt",
                "data": "$data",
            }
        },
    ]
    try:
        df = pd.DataFrame(list(collection.aggregate(pipeline, allowDiskUse=True)))
    except:
        print("Cannot fetch the rssi etc from achieve db...")
    try:
        df.drop("_id", axis=1, inplace=True)
    except:
        print("couln't find _id try with query check first")
    print("retrieved collection successfully!!!")

    return df


# ========================================GATEWAY==========================================


def agg_gw_active(conn, start_time_obj, end_time_obj, database, collection):
    """
    Pipeline Credits: Hira Ahmed
    """
    db = conn[database]
    collection = db[collection]

    if database == "conurets_smart_park_st_pete":
        pipeline = [
            {
                "$match": {
                    "createdDate": {"$gte": start_time_obj, "$lte": end_time_obj},
                    "cmd": "gw",
                }
            },
            {"$unwind": "$gwsBean"},
            {
                "$project": {
                    "eui": "$eui",
                    "ts": "$ts",
                    "gwsTs": "$gwsBean.gwsTs",
                    "gwEui": "$gwsBean.gwEui",
                }
            },
            {
                "$match": {
                    "$expr": {"$eq": ["$ts", "$gwsTs"]},
                    "gwEui": {
                        "$in": ["000800FFFF4A6627", "000800FFFF4B348D", "000800FFFF4B348E"]
                    },
                }
            },
            {"$group": {"_id": "$gwEui", "count": {"$sum": 1}}},
        ]
        df = pd.DataFrame(list(collection.aggregate(pipeline, allowDiskUse=True)))
        archive_flag = False
        if df.empty:
            print("Not found.............")
            df = agg_gw_archive(conn, start_time_obj, end_time_obj)
            archive_flag = True
        else:
            archive_flag = False
        print("retrieved collection successfully!!!")

        return df, archive_flag
    else:
        pipeline = [
            {
                "$match": {
                    "createdDate": {"$gte": start_time_obj, "$lte": end_time_obj},
                    "cmd": "gw",
                }
            },
            {"$unwind": "$gwsBean"},
            {
                "$project": {
                    "eui": "$eui",
                    "ts": "$ts",
                    "gwsTs": "$gwsBean.gwsTs",
                    "gwEui": "$gwsBean.gwEui",
                }
            },
            {"$match": {"$expr": {"$eq": ["$ts", "$gwsTs"]}}},
            {"$group": {"_id": "$gwEui", "count": {"$sum": 1}}},
        ]
        df = pd.DataFrame(list(collection.aggregate(pipeline, allowDiskUse=True)))

        return df


def agg_gw_archive(conn, start_time_obj, end_time_obj):
    """
    Pipeline Credits: Hira Ahmed
    """
    db = conn.conurets_smart_park_st_pete_archive
    collection = db["cd_device_data_log_archive"]

    pipeline = [
        {
            "$match": {
                "createdDate": {"$gte": start_time_obj, "$lte": end_time_obj},
                "cmd": "gw",
            }
        },
        {"$unwind": "$gwsBean"},
        {
            "$project": {
                "eui": "$eui",
                "ts": "$ts",
                "gwsTs": "$gwsBean.gwsTs",
                "gwEui": "$gwsBean.gwEui",
            }
        },
        {
            "$match": {
                "$expr": {"$eq": ["$ts", "$gwsTs"]},
                "gwEui": {
                    "$in": ["000800FFFF4A6627", "000800FFFF4B348D", "000800FFFF4B348E"]
                },
            }
        },
        {"$group": {"_id": "$gwEui", "count": {"$sum": 1}}},
    ]
    df = pd.DataFrame(list(collection.aggregate(pipeline, allowDiskUse=True)))

    print("retrieved collection successfully!!!")
    return df

# ========================================BATTERY STATE==========================================


def battery_state_active(conn, start_time_obj, end_time_obj, database, collection):
    """
    Pipeline Credits: Hira Ahmed
    """

    db = conn[database]
    collection = db[collection]

    pipeline = [
        {
            "$match": {
                "createdDate": {"$gte": start_time_obj, "$lte": end_time_obj},
                "cmd": "gw",
                "data": {"$exists": "true", "$ne": ""},
            }
        },
        {"$project": {"eui": 1, "data": {"$ifNull": ["$data", ""]}, "createdDate": 1}},
        {"$sort": {"createdDate": -1}},
        {
            "$addFields": {
                "BatteryState": {
                    "$switch": {
                        "branches": [
                            {
                                "case": {
                                    "$in": [
                                        {"$substrCP": ["$data", 0, 1]},
                                        ["0", "1", "2", "3", "8", "9", "a", "b"],
                                    ]
                                },
                                "then": "Healthy",
                            },
                            {
                                "case": {
                                    "$in": [
                                        {"$substrCP": ["$data", 0, 1]},
                                        ["4", "5", "6", "7", "c", "d", "e", "f"],
                                    ]
                                },
                                "then": "Weak",
                            },
                        ],
                        "default": "Unknown",
                    }
                }
            }
        },
        {
            "$group": {
                "_id": "$eui",
                "BatteryState": {"$first": "$BatteryState"},
                "CreatedDate": {"$first": "$createdDate"},
            }
        },
        {
            "$group": {
                "_id": "$BatteryState",
                "total": {"$sum": 1},
                "euis": {"$push": "$_id"},
            }
        },
    ]

    df = pd.DataFrame(list(collection.aggregate(pipeline, allowDiskUse=True)))

    if database == "conurets_smart_park_st_pete":
        archive_flag = False
        if df.empty:
            print("Not found.............")
            df = battery_state_archive(conn, start_time_obj, end_time_obj)
            archive_flag = True

        else:
            archive_flag = False
            df = df.rename(columns={"_id": "battery_status"})

        print("retrieved collection successfully!!!")

        return df, archive_flag
    else:
        df = df.rename(columns={"_id": "battery_status"})
        print("retrieved collection successfully!!!")
        return df


def battery_state_archive(conn, start_time_obj, end_time_obj):
    """
    Pipeline Credits: Hira Ahmed
    """

    db = conn.conurets_smart_park_st_pete_archive
    collection = db["cd_device_data_log_archive"]

    pipeline = [
        {
            "$match": {
                "createdDate": {"$gte": start_time_obj, "$lte": end_time_obj},
                "cmd": "gw",
                "data": {"$exists": "true", "$ne": ""},
            }
        },
        {"$project": {"eui": 1, "data": {"$ifNull": ["$data", ""]}, "createdDate": 1}},
        {"$sort": {"createdDate": -1}},
        {
            "$addFields": {
                "BatteryState": {
                    "$switch": {
                        "branches": [
                            {
                                "case": {
                                    "$in": [
                                        {"$substrCP": ["$data", 0, 1]},
                                        ["0", "1", "2", "3", "8", "9", "a", "b"],
                                    ]
                                },
                                "then": "Healthy",
                            },
                            {
                                "case": {
                                    "$in": [
                                        {"$substrCP": ["$data", 0, 1]},
                                        ["4", "5", "6", "7", "c", "d", "e", "f"],
                                    ]
                                },
                                "then": "Weak",
                            },
                        ],
                        "default": "Unknown",
                    }
                }
            }
        },
        {
            "$group": {
                "_id": "$eui",
                "BatteryState": {"$first": "$BatteryState"},
                "CreatedDate": {"$first": "$createdDate"},
            }
        },
        {
            "$group": {
                "_id": "$BatteryState",
                "total": {"$sum": 1},
                "euis": {"$push": "$_id"},
            }
        },
    ]

    df = pd.DataFrame(list(collection.aggregate(pipeline, allowDiskUse=True)))
    print("retrieved collection successfully!!!")
    df = df.rename(columns={"_id": "battery_status"})
    return df


############# Battery state different Format for Downloading Data ##############################

# ========================================BATTERY STATE==========================================


def battery_state_download_active(conn, start_time_obj, end_time_obj, database, collection):
    """
    Pipeline Credits: Hira Ahmed
    """

    db = conn[database]
    collection = db[collection]

    pipeline = [
        {
            "$match": {
                "createdDate": {"$gte": start_time_obj, "$lte": end_time_obj},
                "cmd": "gw",
                "data": {"$exists": "true", "$ne": ""},
            }
        },
        {"$project": {"eui": 1, "data": {"$ifNull": ["$data", ""]}, "createdDate": 1}},
        {"$sort": {"createdDate": -1}},
        {
            "$addFields": {
                "BatteryState": {
                    "$switch": {
                        "branches": [
                            {
                                "case": {
                                    "$in": [
                                        {"$substrCP": ["$data", 0, 1]},
                                        ["0", "1", "2", "3", "8", "9", "a", "b"],
                                    ]
                                },
                                "then": "Healthy",
                            },
                            {
                                "case": {
                                    "$in": [
                                        {"$substrCP": ["$data", 0, 1]},
                                        ["4", "5", "6", "7", "c", "d", "e", "f"],
                                    ]
                                },
                                "then": "Weak",
                            },
                        ],
                        "default": "Unknown",
                    }
                }
            }
        },
        {
            "$group": {
                "_id": "$eui",
                "BatteryState": {"$first": "$BatteryState"},
                "CreatedDate": {"$first": "$createdDate"},
            }
        },
    ]

    df = pd.DataFrame(list(collection.aggregate(pipeline, allowDiskUse=True)))

    if database == "conurets_smart_park_st_pete":
        archive_flag = False
        if df.empty:
            print("Not found.............")
            df = battery_state_download_archive(conn, start_time_obj, end_time_obj)
            archive_flag = True

        else:
            archive_flag = False
            df = df.rename(columns={"_id": "battery_status"})

        print("retrieved collection successfully!!!")

        return df, archive_flag
    else:
        print("retrieved collection successfully!!!")
        df = df.rename(columns={"_id": "battery_status"})
        return df


def battery_state_download_archive(conn, start_time_obj, end_time_obj):
    """
    Pipeline Credits: Hira Ahmed
    """

    db = conn.conurets_smart_park_st_pete_archive
    collection = db["cd_device_data_log_archive"]

    pipeline = [
        {
            "$match": {
                "createdDate": {"$gte": start_time_obj, "$lte": end_time_obj},
                "cmd": "gw",
                "data": {"$exists": "true", "$ne": ""},
            }
        },
        {"$project": {"eui": 1, "data": {"$ifNull": ["$data", ""]}, "createdDate": 1}},
        {"$sort": {"createdDate": -1}},
        {
            "$addFields": {
                "BatteryState": {
                    "$switch": {
                        "branches": [
                            {
                                "case": {
                                    "$in": [
                                        {"$substrCP": ["$data", 0, 1]},
                                        ["0", "1", "2", "3", "8", "9", "a", "b"],
                                    ]
                                },
                                "then": "Healthy",
                            },
                            {
                                "case": {
                                    "$in": [
                                        {"$substrCP": ["$data", 0, 1]},
                                        ["4", "5", "6", "7", "c", "d", "e", "f"],
                                    ]
                                },
                                "then": "Weak",
                            },
                        ],
                        "default": "Unknown",
                    }
                }
            }
        },
        {
            "$group": {
                "_id": "$eui",
                "BatteryState": {"$first": "$BatteryState"},
                "CreatedDate": {"$first": "$createdDate"},
            }
        },
    ]

    df = pd.DataFrame(list(collection.aggregate(pipeline, allowDiskUse=True)))
    print("retrieved collection successfully!!!")
    df = df.rename(columns={"_id": "battery_status"})
    return df


############# Gateway different Format for Downloading Data ##############################

# ========================================GATEWAY==========================================


def agg_gw_download_active(conn, start_time_obj, end_time_obj, database, collection):
    """
    Pipeline Credits: Hira Ahmed
    """
    db = conn[database]
    collection = db[collection]

    if database == "conurets_smart_park_st_pete":
        pipeline = [
            {
                "$match": {
                    "createdDate": {"$gte": start_time_obj, "$lte": end_time_obj},
                    "cmd": "gw",
                }
            },
            {"$unwind": "$gwsBean"},
            {
                "$project": {
                    "eui": "$eui",
                    "createdDate": "$createdDate",
                    "ts": "$ts",
                    "gwsTs": "$gwsBean.gwsTs",
                    "gwEui": "$gwsBean.gwEui",
                }
            },
            {
                "$match": {
                    "$expr": {"$eq": ["$ts", "$gwsTs"]},
                    "gwEui": {
                        "$in": ["000800FFFF4A6627", "000800FFFF4B348D", "000800FFFF4B348E"]
                    },
                }
            },
            {"$project": {"eui": "$eui", "createdDate": "$createdDate", "gwEui": "$gwEui"}},
        ]
        df = pd.DataFrame(list(collection.aggregate(pipeline, allowDiskUse=True)))
        archive_flag = False
        if df.empty:
            print("Not found.............")
            df = agg_gw_download_archive(conn, start_time_obj, end_time_obj)
            archive_flag = True
        else:
            archive_flag = False
        print("retrieved collection successfully!!!")

        return df, archive_flag
    else:
        pipeline = [
            {
                "$match": {
                    "createdDate": {"$gte": start_time_obj, "$lte": end_time_obj},
                    "cmd": "gw",
                }
            },
            {"$unwind": "$gwsBean"},
            {
                "$project": {
                    "eui": "$eui",
                    "createdDate": "$createdDate",
                    "ts": "$ts",
                    "gwsTs": "$gwsBean.gwsTs",
                    "gwEui": "$gwsBean.gwEui",
                }
            },
            {"$match": {"$expr": {"$eq": ["$ts", "$gwsTs"]}}},
            {"$project": {"eui": "$eui", "createdDate": "$createdDate", "gwEui": "$gwEui"}},
        ]
        df = pd.DataFrame(list(collection.aggregate(pipeline, allowDiskUse=True)))
        print("retrieved collection successfully!!!")
        df = df.rename(columns={"_id": "battery_status"})
        return df


def agg_gw_download_archive(conn, start_time_obj, end_time_obj):
    """
    Pipeline Credits: Hira Ahmed
    """
    db = conn.conurets_smart_park_st_pete_archive
    collection = db["cd_device_data_log_archive"]

    pipeline = [
        {
            "$match": {
                "createdDate": {"$gte": start_time_obj, "$lte": end_time_obj},
                "cmd": "gw",
            }
        },
        {"$unwind": "$gwsBean"},
        {
            "$project": {
                "eui": "$eui",
                "createdDate": "$createdDate",
                "ts": "$ts",
                "gwsTs": "$gwsBean.gwsTs",
                "gwEui": "$gwsBean.gwEui",
            }
        },
        {
            "$match": {
                "$expr": {"$eq": ["$ts", "$gwsTs"]},
                "gwEui": {
                    "$in": ["000800FFFF4A6627", "000800FFFF4B348D", "000800FFFF4B348E"]
                },
            }
        },
        {"$project": {"eui": "$eui", "createdDate": "$createdDate", "gwEui": "$gwEui"}},
    ]
    df = pd.DataFrame(list(collection.aggregate(pipeline, allowDiskUse=True)))
    print("retrieved collection successfully!!!")
    return df


# ****************************************************************************************************

# ========================================TOTAL OCCUPANCY==========================================


def total_occupied_active(conn, start_time_obj, end_time_obj, database, collection):
    """
    Pipeline Credits: Hira Ahmed
    """

    db = conn[database]
    collection = db[collection]

    pipeline = [
        {
            "$match": {
                "createdDate": {"$gte": start_time_obj, "$lte": end_time_obj},
                "cmd": "gw",
                "data": {"$exists": "true", "$ne": ""},
            }
        },
        {"$project": {"eui": 1, "data": {"$ifNull": ["$data", ""]}}},
        {"$addFields": {"secondByte_string": {"$substr": ["$data", 1, 1]}}},
        {"$match": {"secondByte_string": "0"}},
        {
            "$addFields": {
                "OccupancyStatus": {
                    "$switch": {
                        "branches": [
                            {
                                "case": {
                                    "$in": [
                                        {"$substrCP": ["$data", 0, 1]},
                                        ["0", "1", "2", "3", "4", "5", "6", "7"],
                                    ]
                                },
                                "then": "Unoccupied",
                            },
                            {
                                "case": {
                                    "$in": [
                                        {"$substrCP": ["$data", 0, 1]},
                                        ["8", "9", "a", "b", "c", "d", "e", "f"],
                                    ]
                                },
                                "then": "Occupied",
                            },
                        ],
                        "default": "Unknown",
                    }
                }
            }
        },
        {"$group": {"_id": "$OccupancyStatus", "count": {"$sum": 1}}},
        {"$match": {"_id": "Occupied"}},
    ]

    df = pd.DataFrame(list(collection.aggregate(pipeline, allowDiskUse=True)))

    if database == "conurets_smart_park_st_pete":
        archive_flag = False
        if df.empty:
            print("Not found.............")
            occupancy_events = total_occupied_archive(conn, start_time_obj, end_time_obj)
            archive_flag = True

        else:
            archive_flag = False
            occupancy_events = int(df["count"].iloc[0])

        print("retrieved collection successfully!!!")

        return occupancy_events, archive_flag
    else:
        print("retrieved collection successfully!!!")
        occupancy_events = int(df["count"].iloc[0])
        return occupancy_events


def total_occupied_archive(conn, start_time_obj, end_time_obj):
    """
    Pipeline Credits: Hira Ahmed
    """

    db = conn.conurets_smart_park_st_pete_archive
    collection = db["cd_device_data_log_archive"]

    pipeline = [
        {
            "$match": {
                "createdDate": {"$gte": start_time_obj, "$lte": end_time_obj},
                "cmd": "gw",
                "data": {"$exists": "true", "$ne": ""},
            }
        },
        {"$project": {"eui": 1, "data": {"$ifNull": ["$data", ""]}}},
        {"$addFields": {"secondByte_string": {"$substr": ["$data", 1, 1]}}},
        {"$match": {"secondByte_string": "0"}},
        {
            "$addFields": {
                "OccupancyStatus": {
                    "$switch": {
                        "branches": [
                            {
                                "case": {
                                    "$in": [
                                        {"$substrCP": ["$data", 0, 1]},
                                        ["0", "1", "2", "3", "4", "5", "6", "7"],
                                    ]
                                },
                                "then": "Unoccupied",
                            },
                            {
                                "case": {
                                    "$in": [
                                        {"$substrCP": ["$data", 0, 1]},
                                        ["8", "9", "a", "b", "c", "d", "e", "f"],
                                    ]
                                },
                                "then": "Occupied",
                            },
                        ],
                        "default": "Unknown",
                    }
                }
            }
        },
        {"$group": {"_id": "$OccupancyStatus", "count": {"$sum": 1}}},
        {"$match": {"_id": "Occupied"}},
    ]

    df = pd.DataFrame(list(collection.aggregate(pipeline, allowDiskUse=True)))

    print("retrieved collection successfully!!!")

    occupancy_events = int(df["count"].iloc[0])

    return occupancy_events


# ****************************************************************************************************
