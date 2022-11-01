import argparse
import tableauserverclient as TSC
import json
import os
import xml.dom.minidom as minidom
import requests
import xmltodict
from treelib import Tree
from operator import itemgetter
from collections import defaultdict

API_VERSION = '3.15'
def main(args):
    project_data = json.loads(args.project_data)
    try:
        # Step 1: Sign in to server.
        print(args.server_url)
        tableau_auth = TSC.TableauAuth(
            args.username, args.password, contentUrl='DataLab')
        server = TSC.Server('https://tableau.devinvh.com',use_server_version=True)
        project_data_json = project_data['workbooks']
	
	
        server.add_http_options({'verify': False})
        with server.auth.sign_in(tableau_auth):
            #site_item = server.sites.get_by_name('DataLab')
            #print(site_item.id, site_item.name, site_item.content_url, site_item.state)
        
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
                        project_id_by_name = get_project_id_by_path_with_tree(args, data['project_path'])
                        print(project_id_by_name)

                        # Step 3: If default project is found, form a new workbook item and publish.
                        if project_id_by_name is not None:
                            new_workbook = TSC.WorkbookItem(
                                name=data['name'], project_id=project_id_by_name, show_tabs=data['show_tabs'])
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
        <site id="e4fc3628-68b4-4a72-a956-9fc3c02192f4" contentUrl="DataLab" />
      </credentials>
    </tsRequest>"""
    response = requests.post(f'{args.server_url}/api/{API_VERSION}/auth/signin', data=payload)
    doc = minidom.parseString(response.text)
    return doc.getElementsByTagName('credentials')[0].getAttribute("token")

def get_all_projects(args):
    token = sign_in(args)
    headers = {
        'X-Tableau-Auth': token
    }
    response = requests.get(f'{args.server_url}/api/{API_VERSION}/sites/e4fc3628-68b4-4a72-a956-9fc3c02192f4/projects?pageSize=1000', headers=headers)
    all_projects_response = xmltodict.parse(response.text)
    print(all_projects_response)
    try:
        all_projects_response = all_projects_response['tsResponse']
        all_projects = all_projects_response['projects']['project']
        return all_projects
    except Exception as e:
        print("Error parsing project response .\n", e)
        return None
    

def get_project_id_by_path_with_tree(args, project_path):
    project_name = project_path.split("/")[-1]

    all_projects = get_all_projects(args)
    project_tree =  parse_projects_to_tree(all_projects)
    project_candidate = find_project_by_name(project_name, all_projects)

    project_path_dict = defaultdict(lambda: None)
    for project in project_candidate:
        nodes = list(project_tree.rsearch(project['@id']))
        nodes.reverse()
        node_path = "/".join([project_tree.get_node(node).data for node in nodes[1:]])
        project_path_dict[node_path] = project['@id']

    return project_path_dict[project_path]

def parse_projects_to_tree(all_projects):
    tree = Tree()
    tree.create_node("tableau", "tableau", data = 'tableau')

    list_root = []
    for project in all_projects:
        if '@parentProjectId' not in project.keys():
            tree.create_node(project['@id'], project['@id'], parent='tableau', data=project['@name'])
            list_root.append(all_projects.index(project))

    index_list = list(set(range(len(all_projects))).difference(list_root))
    all_projects = list(itemgetter(*index_list)(all_projects))


    return tree
	
def find_project_by_name(project_name, list_project):
    temp_projects = list()
    for project in list_project:
        if project['@name'] == project_name:
            temp_projects.append(project)
    return temp_projects


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
