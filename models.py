from wsgiref.util import request_uri
from flask import *
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, VARCHAR, Integer, Column, Date, Float
import os

os.chdir("/Users/taro/projects/portfolio/personal_website/data")

Base = declarative_base()

engine = create_engine("sqlite:///health_care_portal.db")

Base.metadata.bind = engine
Base.metadata.create_all(engine)
DBSession = sessionmaker(bind=engine)
session = DBSession()

class MedCode(Base):
    __tablename__ = 'med_codes'

    id = Column(Integer, primary_key=True)
    code = Column(VARCHAR(5), nullable=False)
    description = Column(VARCHAR(50), nullable=False)
    category = Column(VARCHAR(30), nullable=False)

class Employee(Base):
    __tablename__ = "patient_accounts"

    id = Column(Integer, primary_key=True)
    emp_id = Column(VARCHAR(20), nullable=False)
    title = Column(VARCHAR(30), nullable=False)
    gender = Column(VARCHAR(10), nullable=False)
    last_name = Column(VARCHAR(30), nullable=False)
    first_name = Column(VARCHAR(30), nullable=False)
    salary = Column(Integer, nullable=False)
    city = Column(VARCHAR(20), nullable=False)
    state = Column(VARCHAR(10), nullable=False)

class Transactions(Base):
    __tablename__ = "patient_transactions"

    id = Column(Integer, primary_key=True)
    emp_id = Column(VARCHAR(20), nullable=False)
    trans_id = Column(VARCHAR(15), nullable=False)
    procedure_date = Column(Date, nullable=False)
    medical_code = Column(VARCHAR(10), nullable=False)
    procedure_price = Column(Float, nullable=False)

class Stats:

    def __init__(self, data_x, data_y):
        self.data_x = data_x
        self.data_y = data_y
        self.x_mean = self.mean(self.data_x)
        self.y_mean = self.mean(self.data_y)
        self.sp_xy = sum([(grade - self.x_mean)*(salary - self.y_mean) for grade in self.data_x for salary in self.data_y])
        self.ss_x = sum([(grade - self.x_mean)**2 for grade in self.data_x])
        self.slope = self.sp_xy/self.ss_x
        self.y_intcpt = self.y_mean - self.slope*self.x_mean
        self.tss = sum([(salary - self.y_mean)**2 for salary in self.data_y])
        self.rss = sum([(self.y_intcpt + self.slope * salary) for salary in data_y])
        self.sse = sum([(salary - (self.y_intcpt + self.slope * salary))**2 for salary in data_y])

    def mean(self, data):
        return sum(data)/len(data)
    
class ANOVA(Stats):

    def __init__(self, data_one, data_two):
        pass


    def f_stats(self, group_size, sample_size):
        mse_model = Stats.tss/(group_size-1)
        mse_error = Stats.sse



    # def linear_reg(self):
    #     """
    #     calculates the liner regression of data_x and data_y
    #     """
    #     sp_xy = sum([(grade - self.x_mean)*(salary - self.y_mean) for grade in self.data_x for salary in self.data_y])
    #     ss_x = sum([(grade - self.x_mean)**2 for grade in self.data_x])
    #     b = sp_xy/ss_x
    #     a = self.y_mean - b*self.x_mean
    #     res = a+b*x
    #     return 
        