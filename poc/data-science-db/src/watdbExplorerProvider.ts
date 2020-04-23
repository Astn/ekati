import * as vscode from 'vscode';
import * as fs from 'fs';
import * as path from 'path';
import { WatDbServiceClient } from './types_grpc_pb';
import { GetMetricsRequest, GetMetricsResponse } from './types_pb';
import { ConnectivityState } from '@grpc/grpc-js/build/src/channel';
import { credentials } from '@grpc/grpc-js';
import { promisify } from 'util';

export class WatDbExplorerProvider implements vscode.TreeDataProvider<WatDbTreeItem> {
    localConnection: string = "";
    client: WatDbServiceClient | null = null;

    constructor(private workspaceRoot: string ) {}

    setLocalConnection(path:string) {
        this.localConnection = path;
        // https://github.com/grpc/grpc-node
        // https://blog.stevensanderson.com/2020/01/15/2020-01-15-grpc-web-in-blazor-webassembly/

        console.log(`WatDb - connecting to ${this.localConnection}`);
        this.client?.close();
        this.client = new WatDbServiceClient(this.localConnection, credentials.createInsecure());
		
		var req = new GetMetricsRequest();
		req.addNames("*");


        
        const deadline = Date.now() + 1000;
    
        this.client.waitForReady(deadline, err => {
            console.log('WatDb - Wait For Ready', err);
            if(this.client?.getChannel().getConnectivityState(true) === ConnectivityState.READY){
                console.log('WatDb -  Wait For Ready : Ready');

            		this.client.getMetrics(new GetMetricsRequest(), (err, resp) => {
            			if(err){
            				console.error('WatDb', err);
            			}
            			if(resp){
            				resp.getMetricsList().forEach(metric => {
            					console.log('WatDb', metric.getTime(), metric.getName(), metric.getValue());
            				}); 
            			}
            		});

            } else {
                console.log(`WatDb -  Wait For Ready : ${this.client?.getChannel().getConnectivityState(true)}`);
            }
        });
	

      this.refresh();
  }

  getTreeItem(element: WatDbTreeItem): vscode.TreeItem {
    return element;
  }

  getChildren(element?: WatDbTreeItem): Thenable<WatDbTreeItem[]> {
    if (!this.client) {
      vscode.window.showInformationMessage('Wat Db: No Local Connection');
      return Promise.resolve([]);
    }

    if (element) {
        if(element.id === "_root"){
            let metrics = new WatDbTreeItem("Metrics","",vscode.TreeItemCollapsibleState.Collapsed);
            metrics.id = "_root/metrics";
            let schema = new WatDbTreeItem("Schema","",vscode.TreeItemCollapsibleState.Collapsed);
            schema.id = "_root/schema";
            let stat = new WatDbTreeItem("Stats","",vscode.TreeItemCollapsibleState.Collapsed);
            stat.id = "_root/stats";
            // let metrics = new WatDbTreeItem("Metrics","",vscode.TreeItemCollapsibleState.Collapsed);
            // metrics.id = "_root/metrics";
            return Promise.resolve([
                metrics,
                schema,
                stat
            ]);
        }
        if(element.id === "_root/metrics"){

            const mets : Promise<GetMetricsResponse> = new Promise<GetMetricsResponse>((resolve,reject)=>{
                this.client?.getMetrics(new GetMetricsRequest(), 
                (err, response) => {
                    if (err) reject(err);
                    else resolve(response);
                })
            }); 
                
            return mets
                .then(data => {
                    return data.getMetricsList()
                    .map(metric => new WatDbTreeItem(metric.getName(), metric.getValue().toString(), vscode.TreeItemCollapsibleState.None));
                })
                .catch(err => {
                    return [new WatDbTreeItem("Error:", err, vscode.TreeItemCollapsibleState.None)];
                });
        }
        
        
        return Promise.resolve([
            new WatDbTreeItem("todo:","",vscode.TreeItemCollapsibleState.None)
        ]);
        
    } else {
      // handle root. Let's show our connection as the root
      let root = new WatDbTreeItem(`WatDb@${this.localConnection}`, "Wat Db Server v0.01", vscode.TreeItemCollapsibleState.Collapsed);
      root.id = "_root";
      return Promise.resolve([
          root
      ]);
      
    }
  }

  private _onDidChangeTreeData: vscode.EventEmitter<WatDbTreeItem | undefined> = new vscode.EventEmitter<WatDbTreeItem | undefined>();
  readonly onDidChangeTreeData: vscode.Event<WatDbTreeItem | undefined> = this._onDidChangeTreeData.event;

  refresh(): void {
    this._onDidChangeTreeData.fire();
  }
}

class WatDbTreeItem extends vscode.TreeItem {
  constructor(
    public readonly label: string,
    private version: string,
    public readonly collapsibleState: vscode.TreeItemCollapsibleState
  ) {
    super(label, collapsibleState);
  }

  get tooltip(): string {
    return `${this.label}-${this.version}`;
  }

  get description(): string {
    return this.version;
  }

  iconPath = {
    light: path.join(__filename, '..', '..', 'resources', 'light', 'dependency.svg'),
    dark: path.join(__filename, '..', '..', 'resources', 'dark', 'dependency.svg')
  };
}