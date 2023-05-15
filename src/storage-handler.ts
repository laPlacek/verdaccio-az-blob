import { BlockBlobClient, ContainerClient } from "@azure/storage-blob";
import { pluginUtils } from "@verdaccio/core";
import { Logger, Manifest } from "@verdaccio/types";
import { PassThrough, Readable, Writable } from "stream";
import { join as joinPath} from "path";
import { getError, getTrace } from "./logger-helpers";
import sanitzers from 'sanitize-filename'
import AsyncLock from "async-lock"

const lock = new AsyncLock()

const MANIFEST_BLOB = 'package.json';

export default class AzStorageHandler implements pluginUtils.StorageHandler {
    private manifestBlobClient = this.getManifestBlobClient();

    constructor(
        public packageName: string,
        public logger: Logger,
        private containerClient: ContainerClient) { 
            this.trace('AzStorageHandler created')
        }

    
    async hasPackage(): Promise<boolean> {
        this.trace('hasPackage()');

        return await this.manifestBlobClient.exists();
    }
    
    // actually just removes a file
    async deletePackage(fileName: string): Promise<void> {
        this.trace('deletePackage(@{fileName})', {fileName});

        const blobPath = joinPath(this.packageName, fileName);
        await this.containerClient.deleteBlob(blobPath, { deleteSnapshots: 'include' })
    }

    async createPackage(name: string, manifest: Manifest): Promise<void> {
        this.trace('createPackage(@{name})', {name});
        await this.savePackage(name, manifest);
    }

    async removePackage(): Promise<void> {
        this.trace('removePackage()');
        // not needed?
    }

    async updatePackage(name: string, handleUpdate: (manifest: Manifest) => Promise<Manifest>): Promise<Manifest> {
        this.trace('updatePackage(@{name})', {name});

        return lock.acquire(name, async done => {
            try {
                const manifest = await this._readPackage();
                const updatedManifest = await handleUpdate(manifest);

                done(null, updatedManifest)

            } catch(e: any) {
                this.error('Error while updating the package @{name}', {name})
                done(e);
            }
        });
    }

    async readPackage(name: string): Promise<Manifest> {
        this.trace('readPackage(@{name})', {name});

        try {
            const manifest = await this._readPackage();
            this.trace('readPackage(@{name}): Package red', {name});

            return manifest;
        } catch(e: any) {
            if (e.statusCode === 404) {
                this.trace('readPackage(@{name}): Package does not exist', {name});
                e.code = 404;
                throw e;
            }

            this.error('Error while reading the package @{name}', {name});
            throw e;
        }
    }

    async savePackage(name: string, manifest: Manifest): Promise<void> {
        this.trace('savePackage(@{name})', {name});

        return lock.acquire(name, async done => {
            try {
                const content = JSON.stringify(manifest);
                await this.manifestBlobClient.upload(content, content.length, {});
                this.trace('savePackage(@{name}): Saved', {name});
                done()
            } catch(e: any) {
                this.error('Error while saving the package @{name}', {name});
                done(e);
            }
        });
    }

    async readTarball(name: string, { signal }: {
        signal: AbortSignal;
    }): Promise<Readable> {
        this.trace('readTarball(@{name})', {name});

        return lock.acquire(name, async done => {
            try {
                const client = this.getTarballBlobClient(name);
                
                const readStream = (await client.download(undefined, undefined, { abortSignal: signal })).readableStreamBody!;
                const readable = new Readable().wrap(readStream);
                signal.addEventListener('abort', () => readable.destroy(), {once: true})

                this.trace('readTarball(@{name}): Stream ready', {name});

                done(null, readable)
            } catch(e: any) {
                this.error('Error while reading the tarball @{name}', {name})
                done(e);
            }
        });
    }

    async writeTarball(name: string, { signal }: {
        signal: AbortSignal;
    }): Promise<Writable> {
        this.trace('writeTarball(@{name})', {name});

        return lock.acquire(name, async done => {
            try {
                const client = this.getTarballBlobClient(name);

                const tunnel = new PassThrough();
                signal.onabort = () => tunnel.destroy();
                client.uploadStream(tunnel, undefined, undefined, { abortSignal: signal });

                done(null, tunnel);

                // Verdaccio store expects this event before starting streaming
                process.nextTick(() => {
                    this.trace('writeTarball(@{name}): Stream ready', {name});
                    tunnel.emit('open')
                });
            } catch(e: any) {
                this.error('Error while writing the tarball @{name}', {name});
                done(e);
            }
        });
    }

    async hasTarball(name: string): Promise<boolean> {
        this.trace('hasTarball(@{name})', {name});
        return this.getTarballBlobClient(name).exists();
    }

    private getManifestBlobClient(): BlockBlobClient {
        const packagePath = joinPath(this.packageName, MANIFEST_BLOB);
        return this.containerClient.getBlockBlobClient(packagePath);
    }

    private getTarballBlobClient(tarballName: string): BlockBlobClient {
        const tarballPath = joinPath(this.packageName, sanitzers(tarballName));
        return this.containerClient.getBlockBlobClient(tarballPath);
    }


    private async _readPackage(): Promise<Manifest> {
        const buff = await this.manifestBlobClient.downloadToBuffer();
        return JSON.parse(buff.toString('utf8'));
    }

    private trace(template: string, conf?: any): void {
        getTrace(this.logger)(`[storage/az-blob/storage-handler]: ${template}`, conf);
    }

    private error(template: string, conf?: any): void {
        getError(this.logger)(`[storage/az-blob/storage-handler]: ${template}`, conf);
    }
}