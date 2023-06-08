import quandl
import pandas as pd
import pandas as pd
import matplotlib.pyplot as plt
import mxnet as mx

quandl.ApiConfig.api_key = ''
currensices = pd.read_csv('currencies.csv')

bitfinex_list = dict()
for cur in currensices.values:
    result = quandl.get(f"BITFINEX/{cur[0]}")
    bitfinex_list[cur[0]] = result


with plt.style.context('bmh'):
    plt.figure(figsize=(32, 20))
    x,y = 1,1
    for i, key in enumerate(bitfinex_list.keys()):
        ts_ax = plt.subplot2grid((5,4), (x, y))
        #trade_volume_map[key]['first_cost'].plot(ax=ts_ax, color='purple')
        bitfinex_list[key]['Mid'].plot(ax=ts_ax, color='blue')
        #bitfinex_list[key]['ask_avg_quantity'].plot(ax=ts_ax, color='red')
        ts_ax.set_title(key)
        
        if x==4:
            x=1
            y=y+1
        else:
            x=x+1
        
    plt.tight_layout()
    
    
    
'''    
    
def simple_lstnet_model (iter_train, input_feature_shape, X, Y, win, sz_filt, n_filter, drop):
    ## Для обработки данные должны быть четырех- или пятимерными
    conv_input = mx.sym.reshape(data=X, shape=(0, 1, win, -1))
    ## Сверточный компонент
    ## Дополнение данных до конца временного окна
    
    cnn_output = mx.sym.pad(data=conv_input, 
                            mode='constant', 
                            constant_value=0, 
                            pad_width=(0, 0,
                                       0, 0,
                                       0, sz_filt - 1,
                                       0, 0))
    
    cnn_output = mx.sym.Convolution(data = cnn_output,
                                    kernel = (sz_filt,
                                              input_feature_shape[2]),
                                    num_filter = n_filter)
    
    
    cnn_output = mx.sym.Activation(data = cnn_output, act_type = 'relu')
    
    
    cnn_output = mx.sym.reshape(mx.sym.transpose(data = cnn_output, axes = (0, 2, 1, 3)), shape=(0, 0, 0))
    
    
    cnn_output = mx.sym.Dropout(cnn_output, p = drop)
    
    
    ## Рекуррентный компонент
    stacked_rnn_cells = mx.rnn.SequentialRNNCell()
    stacked_rnn_cells.add(mx.rnn.GRUCell(num_hidden = args.rnn_units))
    outputs, _ = stacked_rnn_cells.unroll(length = win, inputs = cnn_output, merge_outputs = False)
    
    
    rnn_output = outputs[-1]
    n_outputs = input_feature_shape[2]
    cnn_rnn_model = mx.sym.FullyConnected(data=rnn_output,num_hidden=n_outputs)
    
    
    ## AR-компонент
    ar_outputs = []
    for i in list(range(input_feature_shape[2])):
        ar_series = mx.sym.slice_axis(data=X,
                                      axis=2,
                                      begin=i,
                                      end=i+1)
        fc_ar = mx.sym.FullyConnected(data = ar_series, num_hidden = 1)
        ar_outputs.append(fc_ar)
    
    ar_model = mx. sym.concat (*ar_outputs, dim=1)
    
    output = cnn_rnn_model + ar_model
    
    loss_grad = mx.sym.LinearRegressionOutput(data=output, label=Y)
    
    return (loss_grad,
            [v.name for v in iter_train.provide_data],
            [v.name for v in iter_train.provide_label])


'''










