from freelancersdk.session import Session
from freelancersdk.resources.projects.projects import search_projects
from freelancersdk.resources.projects.exceptions import ProjectsNotFoundException
from freelancersdk.resources.projects.helpers import create_search_projects_filter
import csv
from datetime import datetime
import cx_Oracle

def get_today_timestamp_range():
    # Get today's date range in Unix timestamp format
    today = datetime.now()
    start_of_today = int(today.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
    end_of_today = int(today.replace(hour=23, minute=59, second=59, microsecond=999999).timestamp())
    return start_of_today, end_of_today

def sample_search_projects(full_description=False):
    url = 'https://www.freelancer.com'  # Your base URL
    oauth_token = 'SJUFKWBg0G4nd7pDPHl9BH3rPuxRoi'  # Replace with your actual OAuth token
    session = Session(oauth_token=oauth_token, url=url)

    query = '*'
    start_of_today, end_of_today = get_today_timestamp_range()
    
    search_filter = create_search_projects_filter(
        sort_field='time_updated',
        or_search_query=True,
        from_time=start_of_today,
        to_time=end_of_today,
    )

    all_projects = []
    limit = 100
    offset = 0

    while True:
        try:
            p = search_projects(
                session,
                query=query,
                search_filter=search_filter,
                limit=limit,
                offset=offset,
                full_description=full_description,
                frontend_project_statuses=['active']
            )
        except ProjectsNotFoundException as e:
            print('Error message: {}'.format(e.message))
            print('Server response: {}'.format(e.error_code))
            return None
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            return None

        if not p or 'projects' not in p or len(p['projects']) == 0:
            print("No more projects returned from the API.")
            break

        all_projects.extend(p['projects'])
        offset += limit
        print(f"Retrieved {len(p['projects'])} projects, total so far: {len(all_projects)}")
        
    return all_projects

def write_projects_to_csv(projects, filename="projects.csv"):
    if not projects:
        print("No projects to write to CSV.")
        return

    fieldnames = ['id', 'title', 'description', 'time_submitted', 'currency', 'budget', 'status', 'date']

    # Load existing project IDs from the CSV file
    existing_ids = set()
    try:
        with open(filename, mode='r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            existing_ids = {row['id'] for row in reader if 'id' in row}
    except FileNotFoundError:
        print(f"{filename} not found. A new file will be created.")
    except Exception as e:
        print(f"An error occurred while reading {filename}: {e}")

    # Append new data to the CSV file
    try:
        with open(filename, mode='a', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            # If the file was just created, write the header
            if not existing_ids:
                writer.writeheader()

            for project in projects:
                if isinstance(project, dict) and str(project.get('id', '')) not in existing_ids:
                    time_submitted = project.get('time_submitted', '')
                    date = datetime.fromtimestamp(time_submitted).strftime('%Y-%m-%d') if time_submitted else ''

                    writer.writerow({
                        'id': project.get('id', ''),
                        'title': project.get('title', ''),
                        'description': project.get('description', ''),
                        'time_submitted': time_submitted,
                        'currency': project.get('currency', {}).get('code', ''),
                        'budget': project.get('budget', {}).get('minimum', ''),
                        'status': project.get('status', ''),
                        'date': date
                    })
                else:
                    print(f"Duplicate entry for ID {project.get('id', '')}. Skipping write.")
        print(f"Projects data successfully appended to {filename}")
    except Exception as e:
        print(f"An error occurred while writing to {filename}: {e}")

def insert_projects_to_db(projects):
    dsn = cx_Oracle.makedsn('localhost', 1521, service_name='XE')  # Adjust as needed
    connection = cx_Oracle.connect(user='FYP_01', password='system', dsn=dsn)
    
    cursor = connection.cursor()
    
    for project in projects:
        if isinstance(project, dict):
            try:
                # Check if the project ID already exists
                cursor.execute("SELECT COUNT(*) FROM projects WHERE id = :id", {'id': project.get('id', '')})
                count = cursor.fetchone()[0]

                if count == 0:  # Only insert if the ID does not exist
                    insert_query = """
                    INSERT INTO projects (id, title, description, time_submitted, currency, budget, status, posted_date)
                    VALUES (:id, :title, :description, :time_submitted, :currency, :budget, :status, TO_DATE(:posted_date, 'YYYY-MM-DD'))
                    """
                    
                    time_submitted = project.get('time_submitted', '')
                    date = datetime.fromtimestamp(time_submitted).strftime('%Y-%m-%d') if time_submitted else None

                    cursor.execute(insert_query, {
                        'id': project.get('id', ''),
                        'title': project.get('title', ''),
                        'description': project.get('description', ''),
                        'time_submitted': time_submitted,
                        'currency': project.get('currency', {}).get('code', ''),
                        'budget': project.get('budget', {}).get('minimum', ''),
                        'status': project.get('status', ''),
                        'posted_date': date
                    })
                else:
                    print(f"Duplicate entry for ID {project.get('id', '')}. Skipping insert.")
            except cx_Oracle.DatabaseError as e:
                print(f"Database error occurred: {e}")
    
    connection.commit()
    cursor.close()
    connection.close()
    print("Projects data processed.")

# Run the function
projects = sample_search_projects(full_description=True)
if projects:
    print('Number of projects:', len(projects))
    print('Found projects: {}'.format(projects))
    write_projects_to_csv(projects)  # Write the projects to a CSV file
    insert_projects_to_db(projects)  # Insert the projects into the Oracle database