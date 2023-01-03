#!/usr/bin/env python
# coding: utf-8

# Saving, Reading, Grouping, and Manipulating with Pandas

# Set Working Directory

# In[ ]:


import os
os.getcwd()


# In[ ]:


## Change the directory 
os.chdir('E:\BackUp\__H2OResource\MATLAB\MattoGrasso')


# Import Additional Packages

# In[ ]:


import numpy as np
import h5py


# Create Random DataFrame

# In[ ]:


matrix1 = np.random.random(size = (1000,1000))
matrix2 = np.random.random(size = (10000,100))


# Write hdf5 files

# In[ ]:


with h5py.File('E:\\BackUp\\__H2OResource\\MATLAB\\MattoGrasso\\hdf5_data.h5','w') as hdf:
               hdf.create_dataset('dataset1', data=matrix1)
               hdf.create_dataset('dataset2', data=matrix2)


# In[ ]:


pwd


# Read and Print HDF5 files

# In[ ]:


with h5py.File('E:\\BackUp\\__H2OResource\\MATLAB\\MattoGrasso\\hdf5_data.h5','r') as hdf:
    ls = list(hdf.keys())
    print('List of datasets in this file: \n', ls)
    data = hdf.get('dataset1')
    dataset1 = np.array(data)
    print('Shape of dataset1: \n', dataset1.shape)


# In[ ]:


dataset1


# Always important to close the file  "f.close()"

# In[ ]:


f = h5py.File('E:\\BackUp\\__H2OResource\\MATLAB\\MattoGrasso\\hdf5_data.h5','r')
ls = list(f.keys())
f.close()


# In[ ]:


ls


# Create another random dataset

# In[ ]:


matrix1 = np.random.random(size = (1000,1000))
matrix2 = np.random.random(size = (1000,1000))
matrix3 = np.random.random(size = (1000,1000))
matrix4 = np.random.random(size = (1000,1000))


# Read Datasets and Produce Groups and SubGroups inside hdf file

# In[ ]:


with h5py.File('E:\\BackUp\\__H2OResource\\MATLAB\\MattoGrasso\\hdf5_groups.h5', 'w') as hdf:
    G1 = hdf.create_group('Group1')
    G1.create_dataset('dataset1', data = matrix1)
    G1.create_dataset('dataset4', data = matrix4)
    
    G2 = hdf.create_group('Group2/SubGroup1')
    G2.create_dataset('dataset3', data = matrix3)
    
    G3 = hdf.create_group('Group2/SubGroup2')
    G3.create_dataset('dataset2', data = matrix2)


# In[ ]:


matrix2


# In[ ]:


with h5py.File('E:\\BackUp\\__H2OResource\\MATLAB\\MattoGrasso\\hdf5_groups.h5','r') as hdf:
    base_items = list(hdf.items())
    print('Items in the base directory:', base_items)
    G1 = hdf.get('Group1')
    G1_items = list(G1.items())
    print('Items in Group1:', G1_items)
    dataset4 = np.array(G1.get('dataset4'))
    print(dataset4.shape)
    G2 = hdf.get('Group2')
    G2_items = list(G2.items())
    print('Items in Group2:', G2_items)
    G21 = G2.get('/Group2/SubGroup1')
    G21_items = list(G21.items())
    print('Items in Group21:', G21_items)
    dataset3 = np.array(G21.get('dataset3'))
    print(dataset3.shape)
    


# Compress HDF5 Files

# In[ ]:


with h5py.File('E:\\BackUp\\__H2OResource\\MATLAB\\MattoGrasso\\hdf5_groups_compressed.h5','w') as hdf:
    G1 = hdf.create_group('Group1')
    G1.create_dataset('dataset1', data = matrix1, compression="gzip", compression_opts=9)
    G1.create_dataset('dataset4', data = matrix4, compression="gzip", compression_opts=9)
    
    G21 = hdf.create_group('Group2/SubGroup1')
    G21.create_dataset('dataset3', data = matrix3, compression="gzip", compression_opts=9)
                       
    G22 = hdf.create_group('Group2/SubGroup2')
    G22.create_dataset('dataset2', data = matrix2, compression="gzip", compression_opts=9)


# Set and Read Attributes

# In[ ]:


import numpy as np
import h5py


# In[ ]:


# Create the HDF5 file
hdf = h5py.File('E:\\BackUp\\__H2OResource\\MATLAB\\MattoGrasso\\test.h5','w')

# Create the datasets
dataset1 = hdf.create_dataset('dataset1', data=matrix1)
dataset2 = hdf.create_dataset('dataset2', data=matrix2)

# Set Attributes
dataset1.attrs['CLASS'] = 'DATA MATRIX'
dataset1.attrs['VERSION'] = '1.1'

hdf.close()


# In[ ]:


# Read the HDF5 file
hdf = h5py.File('E:\\BackUp\\__H2OResource\\MATLAB\\MattoGrasso\\test.h5','r')
ls = list(hdf.keys())
print('List of datasets in this file: \n', ls)
data = hdf.get('dataset1')
dataset1 = np.array(data)
print('Shape of dataset1: \n', dataset1.shape)
# read the attributes
k = list(data.attrs.keys())
v = list(data.attrs.values())
# print the attributes
print(k[0])
print(v[0])
print(data.attrs[k[0]])

hdf.close()


# Another set of imports

# In[ ]:


import pandas as pd


# In[ ]:


# creates (or opens in append mode) an hdf5 file
hdf = pd.HDFStore('E:\\BackUp\\__H2OResource\\MATLAB\\MattoGrasso\\hdf5_pandas.h5')


# In[ ]:


#  Do not have the Dataset for this, it was unattainable 
#  df1 = pd.read_csv(''E:\\BackUp\\__H2OResource\\MATLAB\\MattoGrasso\\FL_insurance_sample.csv')
#  hdf.put('DF1', df1, format='table', data_columns=True)                   


# In[ ]:


data = {
    "city": ["Tripolo", "Sydney", "Tripoli", "Rome", "Rome", "Tripoli", "Rome", "Sydney", "Sydney"],
    "rank": ["1st", "2nd", "1st", "2nd", "1st", "2nd", "1st", "2nd", "1st"],
    "score1": [44, 48, 39, 41, 38, 44, 34, 54, 61],
    "score2": [67, 63, 55, 70, 64, 77, 45, 66, 72]
}
df2 = pd.DataFrame(data, columns = ['city','rank','score1','score2'])


# In[ ]:


df2


# In[ ]:


hdf.put('DF2Key', df2, format='table', data_columns=True)


# In[ ]:


hdf.close()  #  do not forget to close the hdf5 file


# In[ ]:


# open hdf5 file for reading
hdf = pd.HDFStore('E:\\BackUp\\__H2OResource\\MATLAB\\MattoGrasso\\hdf5_pandas.h5', mode='r')


# In[ ]:


hdf.keys()


# In[ ]:


DF2 = hdf.get('/DF2')


# In[ ]:


type(DF2)


# In[ ]:


DF2.head()


# In[ ]:


pwd


# In[ ]:


import numpy as np
import h5py


# Open Image File

# In[ ]:


# Read the HDF5 file
hdf = h5py.File('E:\\BackUp\\__H2OResource\\MATLAB\\MattoGrasso\\MOD44B.A2000065.h12v10.006.2017081102216.hdf','r')
#ls = list(hdf.keys())
#print('List of datasets in this file: \n', ls)
#data = hdf.get('dataset1')
#dataset1 = np.array(data)
#print('Shape of dataset1: \n', dataset1.shape)
# read the attributes
#k = list(data.attrs.keys())
#v = list(data.attrs.values())
# print the attributes
#print(k[0])
#print(v[0])
#print(data.attrs[k[0]])

#hdf.close()


# In[ ]:




