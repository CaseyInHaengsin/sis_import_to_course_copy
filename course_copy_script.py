import maya
import requests
import json
import pandas as pd
from pandas.io.json import json_normalize
import os
import shutil
import logbook
import sys


app_log = logbook.Logger('App')

#Place API token here
api_key = os.environ['api_key']
header = {'Authorization': 'Bearer ' + api_key}
#Replace {domain here} with Canvas Domain. Should end with domain/api/v1
base_url = ''
#Store the sis account id's you do not want included in your copying
accounts_to_filter = ['accounts_sis_id', 'accounts_sis_id', 'accounts_sis_id', 'accounts_sis_id', 'accounts_sis_id', 'accounts_sis_id']
#accounts_to_filter = ['sis_id_for_account', 'sis_id_for_account', 'sis_id_for_account']
#Store the Canvas ID of the course you would like to copy
course_canvas_id_to_copy = ''
#Store the courses copied with a migration status. This is a report of the courses that made it through the filter and potentially copied.
courses_copied_final = 'courses_copied.csv'
#Location of path
archive_courses_copied_path = ''
#Set to login_id for the user you want to grab course csv's
login_id_to_get_urls = ''
#If you would live to have logging, enter a file name.
filename = 'testlog.txt'


def main():
    #Plug filename into init logging function
    init_logging(filename)
    yesterdays__imports = get_yesterdays_sisimports()
    course_download_urls = get_course_download_urls(yesterdays__imports)
    data_to_check_for_migration = read_to_pandas_and_filter_accounts(course_download_urls)
    data_to_get_activity = check_for_migration(data_to_check_for_migration)
    #TODO - catch an exception and gracefully exit script if, by chance, all courses have already been migrated.
    df_with_activity = build_activity_report(data_to_get_activity)
    courses_to_copy = filter_activity(df_with_activity)
    new_course_copy_df = courses_to_copy.copy()
    new_course_copy_df['migration_status'] = new_course_copy_df.course_id.apply(course_copy)
    new_course_copy_df.to_csv(courses_copied_final)
    try:
        shutil.move(courses_copied_final, f"{archive_courses_copied_path}/{maya.now()}_{courses_copied_final}")
    except Exception as x:
        app_log.exception(x)
    finally:
        msg = 'Program execution finished'
        app_log.notice(msg)

def has_been_migrated(course_id):
    migration_url = f'{base_url}/courses/sis_course_id:{course_id}/content_migrations'
    r = requests.get(migration_url, headers=header)
    if r.ok:
        course_length = json.loads(r.content)
        if len(course_length) > 0:
            return True
        else:
            return False
    else:
        msg = f'The request call to check for a migration has failed with the status code {r.status_code}. Please check this request'
        app_log.warn(msg)


def get_yesterdays_sisimports():
    """
        Returns yesterdays SIS imports
        :return: sis imports from Yesterday
    """
    sis_imports = []
    yesterday = maya.when('yesterday', timezone='EST').iso8601()
    sis_import_url = base_url + '/accounts/self/sis_imports'
    all_params = {'created_since': yesterday, 'per_page': '100'}
    r = requests.get(sis_import_url, headers=header, params=all_params)
    sis_imports.append(json.loads(r.text))
    while 'next' in r.links:
        r = requests.get(url=r.links['next']['url'], headers=header)
        sis_imports.append(json.loads(r.text))
    return sis_imports

def get_course_download_urls(sis_imports):
    """
    Function to get download URLs of courses
    :param sis_imports: array
    :return: a list of course URLs from yesterdays imports
    """
    courses_urls = []
    #first, normalize data in dataframe
    sis_import_df = json_normalize(sis_imports[0]['sis_imports'])
    sis_import_df['should_check_import'] = sis_import_df['user.login_id'].apply(lambda x: True if x == login_id_to_get_urls else False)
    sis_import_df.drop(sis_import_df[sis_import_df['should_check_import'] == False].index, inplace=True)
    attachment = sis_import_df['csv_attachments']
    try:
        for item in attachment:
            try:
                if 'courses.csv' in item[0]['filename']:
                    courses_urls.append(item[0]['url'])
                else:
                    pass
            except TypeError:
                msg = f"The program either failed to locate the csv because of it's name, or the json response for imports has changed. The program did not find the course download urls."
                app_log.warn(msg)
    except Exception as x:
        msg = f"The get_course_download_url function failed with an exception {x}"
        app_log.exception(x)
    return courses_urls

def check_for_migration(course_df):
    course_df['drop_course'] = course_df.course_id.apply(has_been_migrated)
    course_df.drop(course_df[course_df.drop_course == True].index, inplace=True)
    return course_df

def check_account_to_filter(account_row_value):
    """
    Function to check if account should be filtered
    :param account_row_value:
    :return: True if account should be filtered
    """
    if account_row_value in accounts_to_filter:
        return True
    else:
        return False
#Term filtering here
# def check_term_to_filter(term_row_value):
#     if term_row_value in accounts_to_filter:
#         return True
#     else:
#         return False

def get_page_activity(course_id):
    """
    Function to check for page activity. A single page creation is considered activity
    :param course_id:
    :return: Page activity count
    """
    activity_url = '{baseurl}/courses/sis_course_id:{courseid}/pages'.format(baseurl=base_url, courseid=course_id)
    r = requests.get(activity_url, headers=header)
    if r.ok:
        course_url = json.loads(r.content)
        if len(course_url) > 0:
            return True
        else:
            return False
    elif r.status_code == 404:
        return False
    else:
        msg = f"Failed to get page activity for course {course_id}."
        app_log.notice(msg)
        return 'failed with status code: {}'.format(r.status_code)


def get_assignment_activity(course_id):
    """
    Function to check for assignment activity. A single assignment creation is considered activity
    :param course_id:
    :return: Assignment activity count
    """
    activity_url = '{baseurl}/courses/sis_course_id:{courseid}/assignments'.format(baseurl=base_url, courseid=course_id)
    r = requests.get(activity_url, headers=header)
    if r.ok:
        course_url = json.loads(r.content)
        if len(course_url) > 0:
            return True
        else:
            return False
    else:
        msg = f"Failed to get assignment activity for course {course_id}."
        app_log.notice(msg)
        return 'failed with status code: {}'.format(r.status_code)


def get_quiz_activity(course_id):
    """
    Function to check for quiz activity. A single quiz creation is considered activity
    :param course_id:
    :return: Quiz activity count
    """
    activity_url = '{baseurl}/courses/sis_course_id:{courseid}/quizzes'.format(baseurl=base_url, courseid=course_id)
    r = requests.get(activity_url, headers=header)
    if r.ok:
        course_url = json.loads(r.content)
        if len(course_url) > 0:
            return True
        else:
            return False

    else:
        msg = f"Failed to get Quiz activity for course {course_id}."
        app_log.notice(msg)
        return 'failed with status code: {}'.format(r.status_code)


def get_module_activity(course_id):
    """
    Function to get module activity
    :param: course_id:
    :return: True if length is greater than 0
    """
    activity_url = '{baseurl}/courses/sis_course_id:{courseid}/modules'.format(baseurl=base_url, courseid=course_id)
    r = requests.get(activity_url, headers=header)
    if r.ok:
        course_url = json.loads(r.content)
        if len(course_url) > 0:
            return True
        else:
            return False
    else:
        msg = f"Failed to get module activity for course {course_id}."
        app_log.notice(msg)
        return 'failed with status code: {}'.format(r.status_code)


def get_discussion_activity(course_id):
    """
    Function to check for discussion activity. A single discussion creation is considered activity
    :param course_id:
    :return: Discussion activity count
    """
    activity_url = '{baseurl}/courses/sis_course_id:{courseid}/discussion_topics'.format(baseurl=base_url,
                                                                                         courseid=course_id)
    r = requests.get(activity_url, headers=header)
    if r.ok:
        course_url = json.loads(r.content)
        if len(course_url) > 0:
            return True
        else:
            return False
    else:
        msg = f"Failed to get discussion activity for course {course_id}."
        app_log.notice(msg)
        return 'failed with status code: {}'.format(r.status_code)

def read_to_pandas_and_filter_accounts(course_download_urls):
    """
    Function that reads pulled courses and filters accounts and terms
    :return: Dataframe with account information
    """
    try:
        temp_data = pd.concat([pd.read_csv(f) for f in course_download_urls])
        get_all_course_data = temp_data.drop_dupliates(['course_id'], keep='first')
    except Exception as x:
        app_log.exception(x)

    get_all_course_data['remove_account'] = get_all_course_data.account_id.apply(check_account_to_filter)
    data_to_get_activity = get_all_course_data.loc[(get_all_course_data['remove_account'] == False)]
    return data_to_get_activity

# def read_to_pandas_and_filter_accounts_and_terms():
#     temp_data = pd.concat([pd.read_csv(f) for f in course_download_urls])
#     get_all_course_data = temp_data.drop_dupliates(['course_id'], keep='first')
#     get_all_course_data['remove_account'] = get_all_course_data.account_id.apply(check_account_to_filter)
#     get_all_course_data['remove_term'] = get_all_course_data.account_id.apply(check_term_to_filter)
#     data_to_get_activity = get_all_course_data.loc[(previous_day['remove_account'] == False) & (previous_day['remove_term'] == False)]
#     return data_to_get_activity

def build_activity_report(data_to_get_all_activity):
    """
    Function that builds an activity report
    :param data_to_get_all_activity:
    :return: A dataframe with activity being considered for course copy
    """
    data_to_get_all_activity['page_activity'] = data_to_get_all_activity.course_id.apply(get_page_activity)
    data_to_get_all_activity['assignment_activity'] = data_to_get_all_activity.course_id.apply(get_assignment_activity)
    data_to_get_all_activity['quiz_activity'] = data_to_get_all_activity.course_id.apply(get_quiz_activity)
    data_to_get_all_activity['module_activity'] = data_to_get_all_activity.course_id.apply(get_module_activity)
    data_to_get_all_activity['discussion_activity'] = data_to_get_all_activity.course_id.apply(get_discussion_activity)
    return data_to_get_all_activity

def filter_activity(report_with_activity):
    """
    Function that filters any course with activity
    :param report_with_activity:
    :return: A dataframe with filtered courses
    """
    filtered_activity_dataframe = report_with_activity[((report_with_activity.page_activity) == False) & (report_with_activity.assignment_activity == False) & (report_with_activity.quiz_activity == False) & (report_with_activity.module_activity == False) & (report_with_activity.discussion_activity == False)]
    return filtered_activity_dataframe

def course_copy(course_id):
    """
    Function that performs a course copy
    :param course_id:
    :return: A status code or message with status code for failures
    """
    course_copy_url = '{baseurl}/courses/sis_course_id:{courseid}/content_migrations?migration_type=course_copy_importer&settings[source_course_id]={sis_course}'.format(baseurl=base_url, sis_course=course_canvas_id_to_copy, courseid=course_id)
    r = requests.post(course_copy_url, headers=header)
    if r.ok:
        return r.status_code
    else:
        msg = f"Failed to make a course copy for course {course_id}."
        app_log.notice(msg)
        return 'failed with status code: {}'.format(r.status_code)

def init_logging(filename: str = None):
    level = logbook.TRACE

    if filename:
        logbook.TimedRotatingFileHandler(filename, level=level).push_application()
    else:
        logbook.StreamHandler(sys.stdout, level=level).push_application()
    msg = 'Logging initialized, level: {}, mode: {}'.format(
        level,
        "stdout mode" if not filename else 'file mode: ' + filename
    )
    logger = logbook.Logger('Startup')
    logger.notice(msg)


if __name__ == '__main__':
    main()
