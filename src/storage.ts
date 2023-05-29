import { pluginUtils } from "@verdaccio/core";
import { SearchQuery, SearchItem } from "@verdaccio/core/build/search-utils";
import { Token, TokenFilter } from "@verdaccio/types";
import { BlobServiceClient, ContainerClient, StorageSharedKeyCredential } from "@azure/storage-blob";
import { getError, getTrace } from "./logger-helpers";
import AsyncLock from "async-lock"
import AzStorageHandler from "./storage-handler";

const lock = new AsyncLock()

const PACKAGES_LIST_BLOB = 'packages-list.json';
const SECRET_BLOB = 'secret';


export type Config = {
    account: string;
    accountKey: string;
    container: string;
}


function createContainerClient({account, accountKey, container}: Config): ContainerClient {
    return new BlobServiceClient(
        `https://${account}.blob.core.windows.net`,
        new StorageSharedKeyCredential(account, accountKey)
    ).getContainerClient(container);
}


function getConfig(config: Config): Config {
    const c = <Config>{};
    
    for(let prop in config) {
        const v = config[prop];
        c[prop] = process.env[v] || v;
    }
    
    return c;
}


export default class extends pluginUtils.Plugin<Config> implements pluginUtils.Storage<Config> {
    packagesContainerClient = createContainerClient(this.config);

    private packages = null as (string[] | null);
    private packagesClient = this.packagesContainerClient.getBlockBlobClient(PACKAGES_LIST_BLOB);

    private secret = '';
    private secretClient = this.packagesContainerClient.getBlockBlobClient(SECRET_BLOB);

    constructor(public config: Config, public options: pluginUtils.PluginOptions) {
        super(config, options);
        this.config = getConfig(config);
    }
    
    async init(): Promise<void> {
        const { account } = this.config;
        this.options.logger.info({ account }, 'starting AzStorage for account @{account}');
    }

    async getSecret(): Promise<string> {
        return lock.acquire(SECRET_BLOB, async done => {
            if (this.secret) {
                done(null, this.secret);
                return;                
            }

            if (!(await this.secretClient.exists())) {
                done(null, this.secret);
                return;
            }

            const buf = await this.secretClient.downloadToBuffer(); 
            done(null, buf.toString())
        });
    }

    async setSecret(secret: string): Promise<any> {
        return lock.acquire(SECRET_BLOB, async done => {
            await this.secretClient.upload(secret, secret.length);
            this.secret = secret;
            done();
        });
    }

    
    get(): Promise<string[]> {
        return this.getPackages();
    }

    async add(name: string): Promise<void> {
        const packages = await this.getPackages();

        if (packages.indexOf(name) !== -1)
            return;

        await this.updatePackages([...packages, name]);
    }

    async remove(name: string): Promise<void> {
        const packages = await this.getPackages();

        if (packages.indexOf(name) === -1) {
            this.error('Trying to remove non existing package @{name}', {name});
            return;
        }

        await this.updatePackages(packages.filter(p => p !== name));
    }


    getPackageStorage(packageName: string): pluginUtils.StorageHandler {
        return new AzStorageHandler(packageName, this.options.logger, this.packagesContainerClient)
    }

    search(query: SearchQuery): Promise<SearchItem[]> {
        throw new Error("Method not implemented.");
    }
    saveToken(token: Token): Promise<any> {
        throw new Error("Method not implemented.");
    }
    deleteToken(user: string, tokenKey: string): Promise<any> {
        throw new Error("Method not implemented.");
    }
    readTokens(filter: TokenFilter): Promise<Token[]> {
        throw new Error("Method not implemented.");
    }

    private async getPackages(): Promise<string[]> {
        if (this.packages)
            return this.packages;

        try {
            this.trace('Getting packages');

            const buff = await this.packagesClient.downloadToBuffer();
            const packages = JSON.parse(buff.toString()) as string[];

            this.packages = packages;

            return packages;
        } catch(e: any) {
            if (e.statusCode === 404)
                return [];

                
            this.error('Error while getting packages')
            throw e;
        }
    }

    private async updatePackages(newPackages: string[]): Promise<void> {
        try {
            this.trace('Updating packages');

            const str = JSON.stringify(newPackages);
            await this.packagesClient.upload(str, str.length);
            this.packages = newPackages;
        } catch(e) {
            this.error('Error while updating packages')
            throw e;
        }
    }

    private trace(template: string, conf?: any): void {
        getTrace(this.options.logger)(`[storage/az-blob/storage]: ${template}`, conf);
    }

    private error(template: string, conf?: any): void {
        getError(this.options.logger)(`[storage/az-blob/storage]: ${template}`, conf);
    }
}