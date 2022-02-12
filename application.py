import imp
from pydoc import importfile
from api import *
from flask import *
import numpy as np
import sqlite3, os, math, io, base64
import sqlalchemy
from sqlalchemy.engine.base import Connection
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, VARCHAR, Integer, Column, and_, Date, desc, asc, extract, Float, text
from sqlalchemy.sql.sqltypes import DateTime
from sqlalchemy.sql import func
from sqlalchemy.sql.expression import and_
import numpy as np, matplotlib.pyplot as plt
import pandas as pd
from matplotlib.figure import Figure
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas

os.chdir("/Users/taro/projects/portfolio/personal_website")
path = "/Users/taro/projects/portfolio/personal_website/"

Base = declarative_base()

engine = create_engine("sqlite:///portfolio.db")
Base.metadata.bind = engine
Base.metadata.create_all(engine)
DBSession = sessionmaker(bind=engine)
session = DBSession()

app = Flask(__name__) 

func = EmpAnalysis("/Users/taro/projects/portfolio/personal_website/data")

@app.route("/")
def landing_page():
    return render_template("index.html")

@app.route("/home")
def home():
    return render_template("home.html")

@app.route("/salary-analysis")
def salary_analysis():
    texts = func.project_description(path+'salary_analysis_description.txt')
    return render_template("salary_analysis_home.html", texts=texts)

@app.route("/employee/counts")
def emp_counts():
    '''Create a DataFrame for each Salary Grade with Total Count of Employees by Gender'''
    emp_counts_sg = func.emp_counts_sg_df()
    '''Create a DataFrame for each Salary Grade with Total Count of Employees by Dept'''
    emp_counts_dept = func.emp_counts_dept_df()

    '''Create a Pie Chart with Total Count of Employees by Gender for Each Salary Grade'''
    pie_charts_sg=[]
    for grade in emp_counts_sg.index:
        female, male = emp_counts_sg.loc[grade, "Female"], emp_counts_sg.loc[grade, "Male"]
        pie_url_sg = func.chart_pie(female, male, title=f"Employee Count by Gender for Salary Grade {grade}", explode=[0, 0.1], labels=[f"Female: {female}", f"Male: {male}"], colors=("gold", "royalblue"))
        pie_charts_sg.append(pie_url_sg)

    '''Create a Pie Chart with Total Count of Employees by Gender for Each Dept'''
    pie_charts_dept=[]
    for dept in emp_counts_dept.index:
        female, male = emp_counts_dept.loc[dept, "Female"], emp_counts_dept.loc[dept, "Male"]
        pie_url_dept = func.chart_pie(female, male, title=f"Employee Count by Gender for Salary Grade {grade}", explode=[0, 0.1], labels=[f"Female: {female}", f"Male: {male}"], colors=("palevioletred", "deepskyblue"))
        pie_charts_dept.append(pie_url_dept)
    
    return render_template("emp_cnt_by_gender.html", emp_counts_sg=emp_counts_sg, pie_charts_sg=pie_charts_sg, emp_counts_dept=emp_counts_dept, pie_charts_dept=pie_charts_dept)


@app.route("/employees/all")
def emp_list():
    '''Create an alphabetic list of employees by name and department'''
    return render_template("emp_list_all.html", emp_list=func.emp_list_df())


@app.route("/employees/dept")
def emp_list_dept():
    '''Create a list of employees for each department'''
    emp_counts_dept = func.emp_counts_dept_df()

    '''horizontal bar chart with all depts included'''
    fig_hbar = Figure(figsize=(8, 5))
    ax_hbar = fig_hbar.add_subplot(1,1,1)
    ax_hbar.set_title(label=f"Employee Count by Department", fontdict={'color':"black", 'fontsize':20})
    for dept in emp_counts_dept.index:
        count = emp_counts_dept.loc[dept, "Female"]+emp_counts_dept.loc[dept, "Male"]
        ax_hbar.barh(width=count, y=dept)
        buf = io.BytesIO()
        fig_hbar.savefig(buf, format="png")
        image = base64.b64encode(buf.getbuffer()).decode("ascii")
        url_dept = f"img src=data:image/png;base64,{image}"

    return render_template("emp_list_dept.html", emp_list=func.emp_list_df(), url_dept=url_dept)

@app.route("/employees/with/dept")
def emp_with_dept():
    '''Adds Dept_Code & Dept_Name to the columns and lists all emplyees with corresponding values'''
    return render_template("emp_list_with_dept.html", emp_list_with_dept=func.emp_list_with_dept())

@app.route("/employees/active")
def emp_active():
    '''Filter our the employees who had left the organization'''
    today = pd.Timestamp.today().date()
    urls = {}
    dept_name = func.dept_dict()
    emp_active = func.emp_active_df()
    emp_active.sort_values(by="hire_date", ascending=True, inplace=True)
    for n in emp_active.index:
        '''Adds a column 'tenure' and fills it with the duration of service for each active employee'''
        emp_active.loc[n, 'tenure'] = round((today - pd.to_datetime(emp_active.loc[n, "hire_date"]).date()).days/365)
    departments = func.unique_values(emp_active, "dept")
    for dept in departments:
        '''Group employees by tenure and creates a histgram per dept'''
        years = emp_active[emp_active.dept == dept].sort_values(by=["tenure"]).tenure.unique()
        emp_active_dept = emp_active[emp_active.dept == dept]
        fig_hist = Figure(figsize=(10, 6))
        ax_hist = fig_hist.add_subplot(1,1,1)
        ax_hist.set_title(label=f"{dept}: Active Employee Count by Years On The Job", fontdict={'color':"black", 'fontsize':20})
        ax_hist.set_ylim(top=25)
        
        for year in years:
            res = func.histgram(emp_active_dept, col="tenure", key=year, target="name", fig=fig_hist, ax=ax_hist, bin=20)
            urls[dept] = res
    return render_template("emp_active.html", emp_active=emp_active, urls=urls, departments=departments, dept_name=dept_name)

@app.route("/employees/salary")
def emp_salary():
    '''
    Visualizes salary distribution for all employees, grouped by gender, employment satus, salary grade & gender, employment status & gender
    '''
    df = func.emp_status_df(func.emp_active_df())
    all_salaries = func.chart_distribution(df.salary, "Salary Distribution: ALL", 'limegreen')
    salaries_men = func.chart_distribution(df[df.gender == "M"].salary, "Salary Distribution: MEN", 'deepskyblue')
    salaries_women = func.chart_distribution(df[df.gender == "F"].salary, "Salary Distribution: WOMEN", 'hotpink')
    stats = {}
    stats["scatter_plots"]= {}
    stats["scatter_plots"]["mean_by_sg"] = {}
    mean_salary_sg, mean_salary_sg_gender = [], {}

    genders = df.gender.unique()
    for i in genders:
        stats[i] = func.basic_stats(df[df.gender == i].salary)
    
    sg = func.unique_values(df, 's_g')
    for n in sg:
        stats[n] = {}
        stats[n]["ALL"] = func.basic_stats(df[df.s_g == n].salary)
        mean_salary_sg.append(stats[n]["ALL"]["mean"])
    
    stats["scatter_plots"]["mean_by_sg"]["ALL"] = func.scatter_plot(sg, mean_salary_sg, "Mean Salary by Salary Grade", ["red"])
    
    for gender in genders:
        mean_salary_sg_gender[gender] = []
        for n in sg: 
            stats[n][gender] = func.basic_stats(df[(df.s_g == n)&(df.gender == gender)].salary)
            mean_salary_sg_gender[gender].append(stats[n][gender]["mean"])
        stats["scatter_plots"]["mean_by_sg"][gender] = func.scatter_plot(sg, mean_salary_sg_gender[gender], "MALE" if gender == "M" else "FEMALE", ["deepskyblue" if gender == "M" else "hotpink"])

    emp_status = df.status.unique()
    for status in emp_status:
        stats[status] = {}
        stats[status]["ALL"] = func.basic_stats(df[df.status == status].salary)
        for gender in df.gender.unique():
            stats[status][gender] = func.basic_stats(df[(df.status == status)&(df.gender == gender)].salary)
        stats[status]["chart_pie"] = func.chart_pie(stats[status]["M"]["sample_size"], stats[status]["F"]["sample_size"], title=f"'{status}' Employee Count by Gender", explode=[0, 0.1], labels=["Male", "Female"], colors=["deepskyblue", "hotpink"])
        stats[status]["chart_barh_gender"] = func.chart_barh({'width':[stats[status]["M"]["mean"], stats[status]["F"]["mean"]], 'ylabel':["Male", "Female"], 'height':0.8, 'title':status, 'color':["deepskyblue", "hotpink"], 'ylim':None})
    
    all_emp = df.sort_values(by=["s_g", "name"])
    
    return render_template("emp_salary.html", all_salaries=all_salaries, salaries_men=salaries_men, salaries_women=salaries_women, stats=stats, sg=sg, emp_status=emp_status, genders=genders, all_emp=all_emp)

@app.route("/employees/id")
def employees_id():
    '''
    Creates 6 digit numbers all unique to each employee and adds the number to one's first 3 characters of first and last name.
    Then creates a dataframe with the new column 'employee ID'
    '''
    df = func.emp_id_df()
    return render_template("df_with_emp_id.html", df=df)

@app.route("/employees/salary/raise")
def emp_salary_raise():
    '''
    Calculates the raise amount for each employee and group the total by dept and gender
    '''
    df = func.emp_id_df()
    raise_df = func.raise_schedule_df()
    for n in df.index:
        today = pd.Timestamp.today().date()
        years = func.raise_years(round((today - df.loc[n, "hire_date"]).days/365))
        df.loc[n, "raise_amount"] = round(float(df.loc[n, "salary"])*float(raise_df[raise_df.years == years].ratio),2)
        df.loc[n, "new_salary"] = float(df.loc[n, "salary"])+df.loc[n, "raise_amount"]

    departments = func.unique_values(df, "dept")
    info = {}
    info['new_salary'], info['raise_total'], info["raise_by_gender"], info["raise_by_gender_dept"]= {}, {}, {}, {}
    salary_by_dept, raise_by_dept, raise_by_gender, raise_by_gender_dept = {}, {}, {}, {}
    raise_by_gender_dept_url = []
    for dept in departments:
        raise_by_gender_dept[dept] = {}
        salary_by_dept[dept] = df[df.dept == dept].new_salary.sum()
        raise_by_dept[dept] = df[df.dept == dept]['raise_amount'].sum()
        for gender in df.gender.unique():
            raise_by_gender[gender] = df[df.gender == gender].raise_amount.sum()
            raise_by_gender_dept[dept][gender] = (df[(df.gender == gender)&(df.dept == dept)].raise_amount.sum())
        raise_by_gender_dept_url.append(func.chart_pie(raise_by_gender_dept[dept]['F'], raise_by_gender_dept[dept]['M'], title=f"{dept}: Total Raise Allocation by Gender", colors=["deepskyblue", 'hotpink'], explode=[0, 0.1], labels=['Female', 'Male']))

    info['new_salary']['data'] = salary_by_dept
    info['raise_total']['data'] = raise_by_dept      
    info['raise_by_gender']['data'] = raise_by_gender
        
    info['new_salary']['title'] = "Total New Salary per Dept"
    info['new_salary']['xlabel'] = list(info['new_salary']['data'].keys())
    url_new_salary = func.chart_bar(info['new_salary'])
        
    info['raise_total']['title'] = "Sum of Raise Amount per Dept"
    info['raise_total']['colors'] = ['orangered', 'lightcoral', 'gold', 'lightseagreen',  'cornflowerblue', 'mediumpurple', 'slategray']
    info['raise_total']['explode'] = None
    info['raise_total']['labels'] = list(info['raise_total']['data'].keys())
    url_raise_all = func.chart_pie1(info['raise_total'])

    info['raise_by_gender']['title'] = "Total Raise Amount by Gender"
    info['raise_by_gender']['colors'] = ["deepskyblue", 'hotpink']
    info['raise_by_gender']['explode'] = [0, 0.1]
    info['raise_by_gender']['labels'] = list(info['raise_by_gender']['data'].keys())
    url_raise_gender = func.chart_pie1(info['raise_by_gender'])

    return render_template("emp_salary_raise.html", df=df, url_new_salary=url_new_salary, url_raise_all=url_raise_all, url_raise_gender=url_raise_gender, raise_by_gender_dept_url=raise_by_gender_dept_url)
        
@app.route("/employees/promotion")
def emp_promo():
    '''
    Checks each employee's new salary and salary grade to see if one is ready for promotion
    '''
    df = func.calc_raise_df()
    salary_range = func.salary_range_df()
    for n in df.index:
        salary, sg = df.loc[n, "new_salary"], df.loc[n, "s_g"]
        if sg != "7":
            if salary > float(salary_range[salary_range.sg == sg]["end"]):
                df.loc[n, "promotion"] = "PROMOTION DUE"
    df.fillna("", inplace=True)
    # ready = df[df.promotion == "PROMOTION DUE"].emp_id.count()

    promo_by_sg_gender = {}
    promo_by_sg_gender["ALL"] = {}
    promo_by_sg_gender["ALL"]['data'] = {}
    url_promo_by_sg_gender = {}

    grades = func.unique_values(df, "s_g")[:-1]
    for grade in grades:
        promo_by_sg_gender[grade] = {}
        promo_by_sg_gender[grade]['data'] = {}
        for gender in df.gender.unique():
            promo_by_sg_gender["ALL"]['data'][gender] = df[(df.promotion == "PROMOTION DUE")&(df.gender == gender)].emp_id.count()
            promo_by_sg_gender[grade]['data'][gender] = df[(df.s_g == grade)&(df.promotion == "PROMOTION DUE")&(df.gender == gender)].emp_id.count()
        promo_by_sg_gender[grade]['title'] = f"Salary Grade {grade}: Employees Due to Promotion by Gender"
        promo_by_sg_gender[grade]['colors'] = ["deepskyblue", 'hotpink']
        promo_by_sg_gender[grade]['explode'] = [0, 0.1]
        promo_by_sg_gender[grade]['labels'] = ['Female', 'Male']
        url_promo_by_sg_gender[grade] = func.chart_pie1(promo_by_sg_gender[grade])
    
    promo_by_sg_gender["ALL"]['title'] = "Employees Due to Promotion by Gender"
    promo_by_sg_gender["ALL"]['colors'] = ["deepskyblue", 'hotpink']
    promo_by_sg_gender["ALL"]['explode'] = [0, 0.1]
    promo_by_sg_gender["ALL"]['labels'] = ['Female', 'Male']
    by_gender_all = func.chart_pie1(promo_by_sg_gender["ALL"])

    return render_template('emp_promotion.html', df=df, by_gender_all=by_gender_all, url_promo_by_sg_gender=url_promo_by_sg_gender, grades=grades)



if __name__ == "__main__":
    app.run(host="localhost", debug=True, port=5000)