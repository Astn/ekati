// The module 'vscode' contains the VS Code extensibility API
// Import the module and reference it with the alias vscode in your code below
import * as grpc from "@grpc/grpc-js";
import * as vscode from 'vscode';
import { WatDbExplorerProvider } from './watdbExplorerProvider';
import { WatDbServiceClient } from "./types_grpc_pb";
import { GetMetricsRequest } from "./types_pb";
import { ConnectivityState } from "@grpc/grpc-js/build/src/channel";

// this method is called when your extension is activated
// your extension is activated the very first time the command is executed
export function activate(context: vscode.ExtensionContext) {

	const configuredView = vscode.workspace.getConfiguration().get<string>('conf.view.watDbConnection');
	const watDbExplorerProvider = new WatDbExplorerProvider(configuredView ?? "");
	
	if(configuredView && configuredView.length > 0){
		watDbExplorerProvider.setLocalConnection(configuredView);
	}

	vscode.window.registerTreeDataProvider('watDbExplorer', watDbExplorerProvider);
	let refreshEntry = vscode.commands.registerCommand('watDbExplorer.refreshEntry', () => {
		console.log('Extension "watdb" is now active!');
		vscode.window.showInformationMessage('Wat DB - refreshing!');
	  watDbExplorerProvider.refresh();
	});

	let connectLocal = vscode.commands.registerCommand('watDb.connectLocal', async () => {
		const configuredView = vscode.workspace.getConfiguration().get<string>('conf.view.watDbConnection');

		const target = await vscode.window.showQuickPick(
			[
				{ label: 'https', description: 'localhost:5001', target: "localhost:5001" },
				{ label: 'https', description: 'https://localhost:5001', target: "https://localhost:5001" },
				{ label: 'http', description: 'localhost:5000', target: "localhost:5000" },
				{ label: 'http', description: 'http://localhost:5000', target: "http://localhost:5000" },
				{ label: 'withSlashAndServiceName', description: 'localhost:5001/WatDbService', target: "localhost:5001/WatDbService" }
			],
			{ placeHolder: 'Select the endpoint to connect to for your Wat Db.' });
		vscode.window.showInformationMessage(`Saving... Wat Db dir set to ${target?.target}`);
		await vscode.workspace.getConfiguration().update('conf.view.watDbConnection', target?.target, vscode.ConfigurationTarget.Global);
		vscode.window.showInformationMessage(`Saved.. Wat Db dir set to ${target?.target}`);
		
		if(target?.target && target?.target.length > 0){
			watDbExplorerProvider.setLocalConnection(target?.target);
		}
	});

	// Use the console to output diagnostic information (console.log) and errors (console.error)
	// This line of code will only be executed once when your extension is activated
	console.log('Congratulations, your extension "watdb" is now active!');

	// The command has been defined in the package.json file
	// Now provide the implementation of the command with registerCommand
	// The commandId parameter must match the command field in package.json
	let disposable = vscode.commands.registerCommand('watDb.helloWorld', () => {
		// The code you place here will be executed every time your command is executed

		// Display a message box to the user
		vscode.window.showInformationMessage('Hello World from Wat DB!');
	});

	vscode.window.createTreeView('watDbExplorer', {
		treeDataProvider: watDbExplorerProvider
	  });

	context.subscriptions.push(disposable);
	context.subscriptions.push(refreshEntry);
	context.subscriptions.push(connectLocal);
}

// this method is called when your extension is deactivated
export function deactivate() {}
