from airflow.models.variable import Variable

class EnvVariables:  

    def __init__(self, AWS_ACCESS_KEY_ID : str, AWS_SECRET_ACCESS_KEY : str):  
        self.aws_access_key_id = AWS_ACCESS_KEY_ID  
        self.aws_secret_access_key = AWS_SECRET_ACCESS_KEY 
         
    def get_access_key_id(self):  
        return str(Variable.get(self.aws_access_key_id))
    
    def get_secret_access_key(self):
        return str(Variable.get(self.aws_secret_access_key))