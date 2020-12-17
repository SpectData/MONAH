'''
Weaving narrative using intermediate tables.
'''
import os

import pandas as pd


def num_words_lookup(num):
    d = {'num_id': [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
         'num_inwords': ['two', 'three', 'four', 'five',
                         'six', 'seven', 'eight', 'nine',
                         'ten', 'eleven', 'twelve']}
    df = pd.DataFrame(data=d)

    return df.loc[df['num_id'] == num, 'num_inwords'].values[0]

def delay_look_up(num):
    '''
    Look up table for delay
    :return: look up data frame
    '''
    d = {'lb': [-1, 0, 1, 2],
         'ub': [0, 1, 2, 99],
          'text': ['a short delay ', 'a slightly long delay ',
                   'a long delay ', 'a significantly long delay ']}
    df = pd.DataFrame(data=d)

    return df.loc[(df['lb'] <= num) & (num < df['ub']), 'text'].values[0]

def wpm_look_up(num):
    '''
    Look up table for wpm
    :return: look up data frame
    '''
    d = {'lb': [-99, -2, 1, 2],
         'ub': [-2, -1, 2, 99],
          'text': ['very slowly ', 'slowly ',
                   'quickly ', 'very quickly ']}
    df = pd.DataFrame(data=d)

    return df.loc[(df['lb'] <= num) & (num < df['ub']), 'text'].values[0]

def tone_look_up(num):
    '''
    Look up table for tone
    :return: look up data frame
    '''
    d = {'lb': [1, 2, 3],
         'ub': [2, 3, 4],
          'text': ['happily ', 'angrily ',
                   'sadly ']}
    df = pd.DataFrame(data=d)

    return df.loc[(df['lb'] <= num) & (num < df['ub']), 'text'].values[0]

def jaw_drop_look_up(num):
    '''
    Look up table for tone
    :return: look up data frame
    '''
    d = {'lb': [1, 0],
         'ub': [2, 1],
          'text': [' dropped jaw', '']}
    df = pd.DataFrame(data=d)

    return df.loc[(df['lb'] <= num) & (num < df['ub']), 'text'].values[0]

def chin_raiser_look_up(num):
    '''
    Look up table for tone
    :return: look up data frame
    '''
    d = {'lb': [1, 0],
         'ub': [2, 1],
          'text': [' raised chin', '']}
    df = pd.DataFrame(data=d)

    return df.loc[(df['lb'] <= num) & (num < df['ub']), 'text'].values[0]

def upper_lid_raiser_look_up(num):
    '''
    Look up table for tone
    :return: look up data frame
    '''
    d = {'lb': [1, 0],
         'ub': [2, 1],
          'text': [' raised upper lid', '']}
    df = pd.DataFrame(data=d)

    return df.loc[(df['lb'] <= num) & (num < df['ub']), 'text'].values[0]

def stretch_lip_look_up(num):
    '''
    Look up table for tone
    :return: look up data frame
    '''
    d = {'lb': [1, 0],
         'ub': [2, 1],
          'text': [' stretched lip', '']}
    df = pd.DataFrame(data=d)

    return df.loc[(df['lb'] <= num) & (num < df['ub']), 'text'].values[0]

def posiface_look_up(num):
    '''
    Look up table for tone
    :return: look up data frame
    '''
    d = {'lb': [1, 0],
         'ub': [2, 1],
          'text': [' displayed a positive expression', '']}
    df = pd.DataFrame(data=d)

    return df.loc[(df['lb'] <= num) & (num < df['ub']), 'text'].values[0]

def smile_look_up(num):
    '''
    Look up table for tone
    :return: look up data frame
    '''
    d = {'lb': [1, 0],
         'ub': [2, 1],
          'text': [' smiled', '']}
    df = pd.DataFrame(data=d)

    return df.loc[(df['lb'] <= num) & (num < df['ub']), 'text'].values[0]

def weave_narrative(video_name_1, video_name_2, delay, tone, speech_rate,
                    au_action, posiface, smile, parallel_run_settings):
    '''
    Weaves narratives and exports csv
    :return: none
    '''
    # parallel_run_settings = prs.get_parallel_run_settings('marriane_win')

    talkturn = pd.read_csv(os.path.join(parallel_run_settings['csv_path'],
                                        video_name_1 + '_' + video_name_2,
                                        'Stage_2',
                                        'weaved talkturns.csv'))
    talkturn['text'] = talkturn['text'].str.replace(r'[^\w\s]+', '')
    talkturn['text'] = talkturn.text.apply(lambda x: x[:-1] if x[-1:] == ' ' else x)
    talkturn['text'] = talkturn.text.apply(lambda x: x[:-1] if x[-1:] == ' ' else x)
    prosody = pd.read_csv(os.path.join(parallel_run_settings['csv_path'],
                                       video_name_1 + '_' + video_name_2,
                                       'Stage_2',
                                       'talkturn_family_prosody.csv'))
    actions = pd.read_csv(os.path.join(parallel_run_settings['csv_path'],
                                       video_name_1 + '_' + video_name_2,
                                       'Stage_2',
                                       'talkturn_family_actions.csv'))
    dfs = [talkturn, prosody, actions]
    dfr = dfs[0]
    for df_ in dfs[1:]:
        dfr = pd.merge(dfr, df_, how='outer', on=['audio_id', 'speaker', 'talkturn no'])

    if (delay + tone + speech_rate == 0) and (au_action + smile + posiface == 0):
        family = 'v'
    elif (delay + tone + speech_rate >= 1) and (au_action + smile + posiface == 0):
        family = 'vp'
    elif (delay + tone + speech_rate == 0) and (au_action + smile + posiface >= 1):
        family = 'va'
    elif (delay + tone + speech_rate >= 1) and (au_action + smile + posiface >= 1):
        family = 'vpa'
    else:
        family = ''

    dfr['family'] = family
    dfr['text'] = dfr.text.apply(lambda x: x.lower())
    dfr.loc[dfr['happiness_z'] > 2, 'tone'] = 1
    dfr.loc[dfr['anger_z'] > 2, 'tone'] = 2
    dfr.loc[dfr['sadness_z'] > 2, 'tone'] = 3
    dfr = dfr.fillna(0)

    dfr['delay_num_words'] = dfr.delay_ms.apply(lambda x: num_words_lookup(num=round(x/100, 0))
    if x >= 200 and x <= 1200 else (' more than twelve' if x > 1200 else ''))
    dfr['delay_text'] = dfr.apply(lambda x: delay_look_up(num=x['delay_z'])
    if x['delay_ms'] >= 200 else '', axis = 1)
    dfr['wpm_text'] = dfr.wpm_z.apply(lambda x: wpm_look_up(num=x) if x <= -1 or x >= 1 else '')
    dfr['smile_text'] = dfr.smile.apply(lambda x: smile_look_up(num=x))
    dfr['posiface_text'] = dfr.posiface.apply(lambda x: posiface_look_up(num=x))
    dfr['upper_lid_raiser_text'] = dfr.AU05_c.apply(lambda x: upper_lid_raiser_look_up(num=x))
    dfr['chin_raiser_text'] = dfr.AU17_c.apply(lambda x: chin_raiser_look_up(num=x))
    dfr['lip_stretcher_text'] = dfr.AU20_c.apply(lambda x: stretch_lip_look_up(num=x))
    dfr['jaw_drop_text'] = dfr.AU25_c.apply(lambda x: jaw_drop_look_up(num=x))
    dfr['tone_text'] = dfr.tone.apply(lambda x: tone_look_up(num=x) if x > 0 and x <= 3 else '')

    # smile
    dfr['blob_1'] = dfr.apply(lambda x: x['smile_text'] + ' ' if x['smile'] != 0 else '', axis=1)

    # posiface
    dfr['blob_2'] = dfr.apply(lambda x: x['posiface_text'] + ' ' if x['posiface'] != 0
    else '', axis=1)

    # au actions
    dfr['blob_3'] = dfr.apply(lambda x: x['upper_lid_raiser_text'] + x['chin_raiser_text'] +
                                        x['lip_stretcher_text'] + x['jaw_drop_text'] + ' '
    if x['AU05_c'] != 0 or x['AU17_c'] != 0 or x['AU20_c'] != 0 or x['AU25_c'] != 0 else '', axis=1)

    # delay
    dfr['blob_4'] = dfr.apply(lambda x: 'after ' + x['delay_num_words'] + ' hundred milliseconds '
                                        + x['delay_text'] if x['delay_ms'] >= 200 else '', axis=1)

    # speaker
    dfr['blob_5'] = dfr.apply(lambda x: 'the ' + x['speaker'] + ' ', axis=1)

    # tone
    dfr['blob_6'] = dfr.apply(lambda x: x['tone_text'], axis=1)

    # connector
    dfr['blob_7'] = dfr.apply(lambda x: 'and ' if x['tone_text'] != ''
                                                  and x['wpm_text'] != ''
                                                  and tone != 0
                                                  and speech_rate != 0
    else '', axis=1)

    # speech rate
    dfr['blob_8'] = dfr.apply(lambda x: x['wpm_text'], axis=1)
    dfr['blob_9'] = dfr.apply(lambda x: 'the ' + x['speaker'], axis=1)

    # clean
    dfr['blob_1'] = dfr.blob_1.apply(lambda x: '' if smile == 0 else x)
    dfr['blob_2'] = dfr.blob_2.apply(lambda x: '' if posiface == 0 else x)
    dfr['blob_3'] = dfr.blob_3.apply(lambda x: '' if au_action == 0 else x)
    dfr['blob_4'] = dfr.blob_4.apply(lambda x: '' if delay == 0 else x)
    dfr['blob_6'] = dfr.blob_6.apply(lambda x: '' if tone == 0 else x)
    dfr['blob_8'] = dfr.blob_8.apply(lambda x: '' if speech_rate == 0 else x)
    dfr['blob_9'] = dfr.apply(lambda x: '' if x['blob_1'] == ''
                              and x['blob_2'] == ''
                              and x['blob_3'] == ''
                              else x['blob_9'], axis=1)

    # verbatim
    dfr['blob_10'] = dfr.apply(lambda x: 'said ' + str.lower(x['text']) + '.', axis=1)
    dfr['text_blob'] = ''

    for blob in ['blob_9', 'blob_1', 'blob_2', 'blob_3', 'blob_4',
                 'blob_5','blob_6', 'blob_7', 'blob_8', 'blob_10']:
        dfr['text_blob'] = dfr.apply(lambda x: x['text_blob'] + x[blob], axis=1)

    dfr = dfr[['audio_id', 'talkturn no', 'family', 'text_blob']]
    dfr.to_csv(os.path.join(parallel_run_settings['csv_path'],
                            video_name_1 + '_' + video_name_2,
                            "Stage_3",
                            "narrative_fine.csv"),
               index=False)
    print(dfr)

if __name__ == '__main__':
    weave_narrative(video_name_1='Ses01F_F',
                    video_name_2='Ses01F_M',
                    delay=1,
                    tone=1,
                    speech_rate=1,
                    au_action=1,
                    posiface=1,
                    smile=1)
