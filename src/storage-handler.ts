import { ContainerClient } from "@azure/storage-blob";
import { pluginUtils } from "@verdaccio/core";
import { Logger, Manifest } from "@verdaccio/types";
import { PassThrough, Readable, Writable } from "stream";
import { getError, getTrace } from "./logger-helpers";
import { nextTick } from "process";

const MANIFEST_BLOB = 'package.json';

export default class AzStorageHandler implements pluginUtils.StorageHandler {
    private manifestBlobClient = this.containerClient.getBlockBlobClient(this.getPath(MANIFEST_BLOB));

    constructor(
        public packageName: string,
        public logger: Logger,
        private containerClient: ContainerClient) { 
            this.trace('AzStorageHandler created')
        }

    
    async hasPackage(): Promise<boolean> {
        return await this.manifestBlobClient.exists();
    }
    
    // actually just removes a file
    async deletePackage(name: string): Promise<void> {
        const deleteBlob = (path: string) => {
            this.trace('Removing file @{path}', {path});
            return this.containerClient.deleteBlob(path, { deleteSnapshots: 'include' });
        }

        await deleteBlob(`${this.packageName}/${name}`);
    }

    async createPackage(name: string, manifest: Manifest): Promise<void> {
        this.trace('Creating package @{name}', {name});
        await this.savePackage(name, manifest);
    }

    async removePackage(): Promise<void> {
        // not needed?
    }

    async updatePackage(name: string, handleUpdate: (manifest: Manifest) => Promise<Manifest>): Promise<Manifest> {
        this.trace('Updating manifest of @{name}', {name});

        try {
            const manifest = await this._readPackage();
            const updatedManifest = await handleUpdate(manifest);

            return updatedManifest;

        } catch(e: any) {
            this.error('Error while updating the package @{name}', {name})
            throw e;
        }
    }

    async readPackage(name: string): Promise<Manifest> {
        this.trace('Reading manifest of @{name}', {name});

        try {
            const manifest = await this._readPackage();

            return manifest;
        } catch(e: any) {
            if (e.statusCode === 404) {
                this.error('Package @{name} does not exist', {name});
                e.code = 404;
                throw e;
            }

            this.error('Error while reading the package @{name}', {name});
            throw e;
        }
    }

    async savePackage(name: string, manifest: Manifest): Promise<void> {
        this.trace('Saving manifest of @{name}', {name});

        try {
            const content = JSON.stringify(manifest);
            await this.manifestBlobClient.upload(content, content.length, {});
            return;
        } catch(e: any) {
            this.error('Error while saving the package @{name}', {name});
            throw e;
        }
    }

    async readTarball(name: string, { signal }: {
        signal: AbortSignal;
    }): Promise<Readable> {
        this.trace('Reading tarbal @{name}', {name});

        try {
            const client = this.containerClient.getBlockBlobClient(this.getTarballPath(name));
            
            const readStream = (await client.download(undefined, undefined, { abortSignal: signal })).readableStreamBody!;
            const readable = new Readable().wrap(readStream);
            
            signal.addEventListener('abort', () => readable.destroy(), {once: true});
            
            nextTick(() => readable.emit('open'));

            return readable;
        } catch(e: any) {
            this.error('Error while reading packed tarball @{name}', {name})
            throw e;
        }
    }

    async hasTarball(name: string): Promise<boolean> {
        return this.containerClient.getBlockBlobClient(this.getTarballPath(name)).exists();
    }

    async writeTarball(name: string, { signal }: {
        signal: AbortSignal;
    }): Promise<Writable> {
        this.trace('Writing tarbal @{name}', {name});

        try {
            const client = this.containerClient.getBlockBlobClient(this.getTarballPath(name));

            const tunnel = new PassThrough();
            signal.onabort = () => tunnel.destroy();
            client.uploadStream(tunnel, undefined, undefined, { abortSignal: signal });

            process.nextTick(() => tunnel.emit('open'));

            return tunnel;
        } catch(e: any) {
            this.error('Error while writing the tarball @{name}', {name});
            throw e;
        }
    }

    private tarballName(name: string): string {
        return name.split('-').pop()!;
    }

    private getPath(name: string): string {
        return `${this.packageName}/${name}`;
    }

    private getTarballPath(name: string): string {
        return this.getPath(this.tarballName(name));
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