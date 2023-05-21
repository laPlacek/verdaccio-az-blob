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


export type AzStorageConfig = {
    account: string;
    accountKey: string;
    container: string;
    storeUnpacked?: boolean;
}


function createContainerClient({account, accountKey, container} : {account: string, accountKey: string, container: string}): ContainerClient {
    return new BlobServiceClient(
        `https://${account}.blob.core.windows.net`,
        new StorageSharedKeyCredential(account, accountKey)
    ).getContainerClient(container);
}

function getAccountKey({accountKey}: AzStorageConfig): string {
    if (!accountKey)
        throw Error('Account key not set');

    return process.env[accountKey] || accountKey;
}


export default class AzStorage extends pluginUtils.Plugin<AzStorageConfig> implements pluginUtils.Storage<AzStorageConfig> {
    private accountKey = getAccountKey(this.config);

    packagesContainerClient = createContainerClient({
        account: this.config.account,
        accountKey: this.accountKey,
        container: this.config.container
    });

    private settingsContainerClient = createContainerClient({
        account: this.config.account,
        accountKey: this.accountKey,
        container: `${this.config.container}-settings`
    });

    private packages = null as (string[] | null);
    private packagesClient = this.packagesContainerClient.getBlockBlobClient(PACKAGES_LIST_BLOB);

    private secret = '';
    private secretClient = this.settingsContainerClient.getBlockBlobClient(SECRET_BLOB);

    constructor(
        public config: AzStorageConfig,
        public options: pluginUtils.PluginOptions) { 
            super(config, options)
    }
    
    async init(): Promise<void> {
        const { account } = this.config;
        this.options.logger.info({ account }, 'starting AzStorage for account @{account}')
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
        return lock.acquire<string[]>(PACKAGES_LIST_BLOB, async done => {
            try {
                const names = await this.getPackages();
                done(null, names);
            } catch(e: any) {
                done(e);
            }
        })
    }

    add(name: string): Promise<void> {
        return lock.acquire(PACKAGES_LIST_BLOB, async done => {
            try {
                const packages = await this.getPackages();

                if (packages.indexOf(name) !== -1) {
                    done();
                    return;
                }

                await this.updatePackages([...packages, name]);

                done();
            } catch(e: any) {
                done(e);
            }
        })
    }

    async remove(name: string): Promise<void> {
        return lock.acquire(PACKAGES_LIST_BLOB, async done => {
            try {
                const packages = await this.getPackages();

                if (packages.indexOf(name) === -1) {
                    this.error('Trying to remove non existing package @{name}', {name});
                    return;
                }
    
                await this.updatePackages(packages.filter(p => p !== name));
    
                done();
            } catch(e: any) {
                done (e);
            }
        })
    }


    getPackageStorage(packageName: string): pluginUtils.StorageHandler {
        return new AzStorageHandler(packageName, this.options.logger, this.packagesContainerClient, this.config.storeUnpacked)
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