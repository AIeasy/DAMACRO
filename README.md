## DAMACRO

This repository presents the DAMACRO framework which utilize pre-trained classifiers to clustering the data into different cluster for compression ratio improvment

### Code usage

Run the scripts by using following command

``` python
python main.py
```

### Parameter setting
Here is some parameters that can be changed to get experiment results.
``` python
    original_data_sizes=[50.12,40.09,45.11] 
    scaler_data_size = 71.78
    file_path = 'data/original'
    dataset = 'flight_adjusted' 

    train_percents = [0.8,0.2,0.1] 
    model_path = 'models'
    model_names = ['DecisionTree','GaussianNB','LogisticRegression','MLP','QDA']
    alg = ['gzip','lz4','zstd']
    overlaps = ['insert','no_overlap','update']
    cat_i = [0, 1, 3, 6]#[0, 5]#[0, 1, 2, 3] #[0, 5]#[0, 5]#[0, 1, 3, 6]
    num_i = [2, 5]#[3, 6]#[3, 6]#[3, 6]#[2, 5]
    delimiter = '|'
    chunk_size = 10000

    target_path = '' #put target path here
    networkspeeds =[25,20,15,10,5]#,20,15,10,5,2,20,15
    target_ip = '' #put target ip here
    target_user = '' #put user name here
```


