from Python.Data_Preprocessing.Stage_3 import narrative_fine as nrf
from Python.Data_Preprocessing.config import dir_config as dcf


class TestNRF:
    parallel_run_settings = dcf.get_parallel_run_settings('joshua_linux')

    dfr = nrf.weave_narrative(video_name_1='Ses01F_F',
                              video_name_2='Ses01F_M',
                              delay=1,
                              tone=1,
                              speech_rate=1,
                              au_action=1,
                              posiface=1,
                              smile=1, parallel_run_settings=parallel_run_settings)

    def test_init(self):
        assert len(self.dfr) > 0

    def test_sorted(self):
        source = self.dfr['talkturn no']
        sorted = self.dfr['talkturn no'].sort_values()
        assert source.equals(sorted)
