#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv

class ETLEngine:
    def __init__(self, data_path='data/event_data'):
        self.data_path = data_path
        self.processed_file = 'data/processed/event_datafile_new.csv'
        
    def process_raw_data(self):
        """Process raw event data files into a single denormalized CSV"""
        file_path_list = []
        
        # Get all CSV files from event_data directory
        for root, dirs, files in os.walk(self.data_path):
            file_path_list.extend(glob.glob(os.path.join(root, '*.csv')))
        
        full_data_rows_list = []
        
        # Process each file
        for file_path in file_path_list:
            with open(file_path, 'r', encoding='utf8', newline='') as csvfile:
                csvreader = csv.reader(csvfile)
                next(csvreader)  # Skip header
                
                for line in csvreader:
                    full_data_rows_list.append(line)
        
        # Create processed directory if it doesn't exist
        os.makedirs(os.path.dirname(self.processed_file), exist_ok=True)
        
        # Write consolidated CSV
        csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)
        
        with open(self.processed_file, 'w', encoding='utf8', newline='') as f:
            writer = csv.writer(f, dialect='myDialect')
            writer.writerow([
                'artist', 'firstName', 'gender', 'itemInSession', 'lastName', 
                'length', 'level', 'location', 'sessionId', 'song', 'userId'
            ])
            
            for row in full_data_rows_list:
                if row[0] == '':  # Skip empty artist rows
                    continue
                writer.writerow((
                    row[0], row[2], row[3], row[4], row[5], 
                    row[6], row[7], row[8], row[12], row[13], row[16]
                ))
        
        print(f"Processed data saved to: {self.processed_file}")
        return self.processed_file
