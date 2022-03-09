from coconnect.tools.bclink_helpers import BCLinkHelpers
from .local import LocalDataCollection
import io

class BCLinkDataCollection(LocalDataCollection):
    def __init__(self,bclink_settings,**kwargs):
            
        self.logger.info('setup bclink collection')
        self.bclink_helpers = BCLinkHelpers(**bclink_settings)

        super().__init__(**kwargs)


    def write(self,*args,**kwargs):
        f_out = super().write(*args,**kwargs)
        destination_table = args[0]
        self.bclink_helpers.load_table(f_out,destination_table)
        
    def load_global_ids(self):
        data = self.bclink_helpers.get_global_ids()
        if not data:
            return
        if len(data.splitlines()) == 0:
            return

        sep = self.get_separator()
        data = io.StringIO(data)
        df_ids = pd.read_csv(io.StringIO(stdout),
                             sep=sep).set_index('TARGET_SUBJECT')['SOURCE_SUBJECT']
        
        return df_ids.to_dict()
