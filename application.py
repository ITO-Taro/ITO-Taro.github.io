from models import *
from api import *
from flask import *
import os
from sqlalchemy.orm import sessionmaker

data_path = "data/"

# Base = declarative_base()
# engine = create_engine("sqlite:///portfolio.db")
Base.metadata.bind = engine
Base.metadata.create_all(engine)
DBSession = sessionmaker(bind=engine)
session = DBSession()

app = Flask(__name__) 
SQLAlchemy().init_app(app)

emp = EmpAnalysis(data_path)
health = HealthCarePortal(data_path)

@app.route("/")
def landing_page():
    return render_template("index.html")

@app.route("/home")
def home():
    return render_template("home.html")

@app.route("/salary-home")
def salary_analysis():
    description = emp.description
    # texts = emp.project_description(path+'salary_analysis_description.txt')
    return render_template("salary_analysis_home.html", texts=description)

@app.route("/salary/employee/counts")
def emp_counts():
    """
    1. Create a DataFrame for each Salary Grade with Total Count of Employees by Gender
    2. Create a DataFrame for each Salary Grade with Total Count of Employees by Dept
    3. Create a Pie Chart with Total Count of Employees by Gender for Each Salary Grade
    4. Create a Pie Chart with Total Count of Employees by Gender for Each Dept
    """
    
    res = emp.employee_counts

    return render_template("emp_cnt_by_gender.html", emp_counts_sg=res['emp_counts_sg'], pie_charts_sg=res['pie_charts_sg'], emp_counts_dept=res['emp_counts_dept_gen'], pie_charts_dept=res['pie_charts_dept'])


@app.route("/salary/employees/all")
def emp_list():
    '''Create an alphabetic list of employees by name and department'''
    return render_template("emp_list_all.html", emp_list=emp.df)


@app.route("/salary/employees/dept")
def emp_list_dept():

    res = emp.emp_counts_by_dept_chart_barh

    return render_template("emp_list_dept.html", emp_list=emp.df, url_dept=res)

@app.route("/salary/employees/with/dept")
def emp_with_dept():
    '''Adds Dept_Code & Dept_Name to the columns and lists all emplyees with corresponding values'''
    return render_template("emp_list_with_dept.html", emp_list_with_dept=emp.emp_list_with_dept())

@app.route("/salary/employees/active")
def emp_active():
    """
    1. Filters out employees who are no longer with the company and create data frame
    2. Calculate the tenure for each actuve employee (use today's date)
    3. Counts the number of emp per dept and group them by the tenure years
    4. Create a bar chart with the result from #3
    5. List all emp sorted by hire date in ascending order for each dept
    """
    data = emp.active_emp_page

    return render_template("emp_active.html", emp_active=data['emp_active'], urls=data['urls'], departments=data['dept'], dept_name=data['dept_name'])

@app.route("/salary/employees/salary")
def emp_salary():
    '''
    Visualizes salary distribution for all employees, grouped by gender, employment satus, salary grade & gender, employment status & gender
    '''
    res = emp.emp_salary_scatter_plots
    
    return render_template("emp_salary.html", all_salaries=res['all_salaries'], salaries_men=res['salaries_men'], \
        salaries_women=res['salaries_women'], stats=res['stats'], sg=res['sg'], \
            emp_status=res['emp_status'], genders=res['genders'], all_emp=res['all_emp'])

@app.route("/salary/employees/id")
def employees_id():
    '''
    Creates 6 digit numbers all unique to each employee and adds the number to one's first 3 characters of first and last name.
    Then creates a dataframe with the new column 'employee ID'
    '''
    df = emp.emp_id_df()
    return render_template("df_with_emp_id.html", df=df)

@app.route("/salary/employees/salary/raise")
def emp_salary_raise():
    '''
    Calculates the raise amount for each employee and group the total by dept and gender
    '''
    res = emp.salary_raise_page()
   
    return render_template("emp_salary_raise.html", df=res['df'], url_new_salary=res['url_new_salary'], url_raise_all=res['url_raise_all'], url_raise_gender=res['url_raise_gender'], raise_by_gender_dept_url=res['url_raise_by_gender_dept'])
        
@app.route("/salary/employees/promotion")
def emp_promo():
    '''
    Checks each employee's new salary and salary grade to see if one is ready for promotion
    '''
    res = emp.salary_promotion

    return render_template('emp_promotion.html', df=res['df'], by_gender_all=res['by_gender_all'], url_promo_by_sg_gender=res['url_promo_by_sg_gender'], grades=res['grades'])

@app.route("/health-care-portal-home")
def health_care_portal_home():

    description = health.description

    return render_template("health_care_portal_home.html", texts=description)

@app.route("/med_codes", methods=['get', 'post'])
def search_by_med_code():
    if request.method == 'POST':
        input = str(request.form["med_code"]).strip(" ")
        return health.med_code_search(input)

    else:
        return render_template("med_codes.html")

@app.route("/med_descript", methods=['get', 'post'])
def search_med_descript():
    if request.method == 'POST':
        input = str(request.form["key_word"]).strip(" ")
        return health.med_descript_search(input)
    else:
        return render_template("med_descript.html")

@app.route("/emp_search", methods=['get', 'post'])
def emp_id_search():
    if request.method == 'POST':
        input = str(request.form["emp_id"]).strip(" ")
        return health.emp_search_by_id(input)
    else:
        return render_template('emp_search.html')

@app.route("/emp_search/last", methods=['get', 'post'])
def emp_search_lastname():

    if request.method == 'POST':
        input = str(request.form["last_name"]).strip(" ")
        return health.emp_search_by_last(input)
    else:
        return render_template('emp_search_last.html')

@app.route("/emp_search/salary_range", methods=['get', 'post'])
def salary_range():
    if request.method == 'POST':
        input_min, input_max = request.form["min"].strip(" "), request.form["max"].strip(" ")
        return health.emp_search_salary_range(input_min, input_max)
    else:
        return render_template('emp_Search_salary_range.html')

if __name__ == "__main__":
    app.run(host="localhost", debug=True, port=5000)