import argparse
import tableauserverclient as TSC
import json
import os
import xml.dom.minidom as minidom
import requests
import xmltodict

API_VERSION = '3.9'
def main(args):
    project_data = json.loads(args.project_data)
    try:
        # Step 1: Sign in to server.
        print(args.server_url)
        tableau_auth = TSC.TableauAuth(
            args.username, args.password,'')
        server = TSC.Server('https://tableau.devinvh.com',use_server_version=True)
        project_data_json = project_data['workbooks']
        server.add_http_options({'verify': False})
        with server.auth.sign_in(tableau_auth):
            site_item = server.sites.get_by_name('Enterprise')
            print(site_item.id, site_item.name, site_item.content_url, site_item.state)
        
            try:
                for data in project_data_json:
                    wb_path = os.path.dirname(os.path.realpath(__file__)).rsplit(
                        '/', 1)[0] + "/workbooks/" + data['file_path']

                    if data['project_path'] is None:
                        error = f"The project project_path field is Null in JSON Template."
                        print(
                            f"{data['file_path']} workbook is not published.")
                        raise LookupError(error)
                        exit(1)
                    else:
                        # Step 2: Get all the projects on server, then look for the default one.
                        project_name_by_site_id = get_all_projects(args)
                        print(project_name_by_site_id)
                        all_projects, pagination_item = server.projects.get()
                        project = next(
                            (project for project in all_projects if project.name == data['project_path']), None)

                        # Step 3: If default project is found, form a new workbook item and publish.
                        if project is not None:
                            new_workbook = TSC.WorkbookItem(
                                name=data['name'], project_id=project.id, show_tabs=data['show_tabs'])
                            new_workbook = server.workbooks.publish(
                                new_workbook, wb_path, 'Overwrite', hidden_views=data['hidden_views'])
                            if data['tags'] is not None:
                                new_workbook.tags = set(data['tags'])
                                new_workbook = server.workbooks.update(
                                    new_workbook)
                            print(
                                f"\nWorkbook :: {data['file_path']} :: published in {data['project_path']} project")
                        else:
                            error = f"The project for {data['file_path']} workbook could not be found."
                            print(
                                f"{data['file_path']} workbook is not published.")
                            raise LookupError(error)
                            exit(1)

            except Exception as e:
                print(
                    f"{wb_path.rsplit('/', 1)[1]} Workbook not published.\n", e)
                exit(1)

    except Exception as e:
        print("Signin error.\n", e)
        exit(1)
        
def sign_in(args):
    payload = \
    f"""<tsRequest>
      <credentials name="{ args.username }" password="{ args.password }" >
        <site contentUrl="" />
      </credentials>
    </tsRequest>"""
    print(payload)
    response = requests.post(f'{args.server_url}/api/{API_VERSION}/auth/signin', data=payload)
    print(response)
    doc = minidom.parseString(response.text)
    return doc.getElementsByTagName('credentials')[0].getAttribute("token")

def get_all_projects(args):
    token = sign_in(args)
    headers = {
        'X-Tableau-Auth': token
    }
    response = requests.get(f'{args.server_url}/api/{API_VERSION}/sites/Enterprise/projects?pageSize=1000', headers=headers)
    all_projects_response = xmltodict.parse(response.text)
    try:
        all_projects_response = all_projects_response['tsResponse']
        all_projects = all_projects_response['projects']['project']
        return all_projects
    except Exception as e:
        print("Error parsing project response .\n", e)
        return None

if __name__ == '__main__':
    parser = argparse.ArgumentParser(allow_abbrev=False)

    parser.add_argument('--username', action='store',
                        type=str, required=True)
    parser.add_argument('--password', action='store',
                        type=str, required=True)
    parser.add_argument('--server_url', action='store',
                        type=str, required=True)
    parser.add_argument('--project_data', action='store',
                        type=str, required=True)

    args = parser.parse_args()
    main(args)
