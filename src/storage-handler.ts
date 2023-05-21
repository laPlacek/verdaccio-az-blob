import { BlockBlobClient, ContainerClient, ContainerListBlobFlatSegmentResponse } from "@azure/storage-blob";
import { pluginUtils } from "@verdaccio/core";
import { Logger, Manifest } from "@verdaccio/types";
import { PassThrough, Readable, Writable } from "stream";
import { getError, getTrace } from "./logger-helpers";
import { pack, extract } from "tar-stream";
import zlib from 'zlib';

const MANIFEST_BLOB = 'package.json';

export default class AzStorageHandler implements pluginUtils.StorageHandler {
    private manifestBlobClient = this.containerClient.getBlockBlobClient(this.getPath(MANIFEST_BLOB));

    constructor(
        public packageName: string,
        public logger: Logger,
        private containerClient: ContainerClient,
        public storeUnpacked?: boolean) { 
            this.trace('AzStorageHandler created')
        }

    
    async hasPackage(): Promise<boolean> {
        this.trace('hasPackage()');

        return await this.manifestBlobClient.exists();
    }
    
    // actually just removes a file
    async deletePackage(name: string): Promise<void> {
        const deleteBlob = (path: string) => {
            this.trace('Removing file @{path}', {path});
            return this.containerClient.deleteBlob(path, { deleteSnapshots: 'include' });
        }

        if (!this.storeUnpacked || !name.endsWith('.tgz')) {
            await deleteBlob(`${this.packageName}/${this,name}`);
            return;
        }

        const dirName = this.getDirName(name);
        const prefix = this.getPath(dirName);
        for await (const { name } of this.containerClient.listBlobsFlat({ prefix })) {
            await deleteBlob(name);
        }
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

        try {
            const content = JSON.stringify(manifest);
            await this.manifestBlobClient.upload(content, content.length, {});
            this.trace('savePackage(@{name}): Saved', {name});
            return;
        } catch(e: any) {
            this.error('Error while saving the package @{name}', {name});
            throw e;
        }
    }

    async readTarball(name: string, { signal }: {
        signal: AbortSignal;
    }): Promise<Readable> {
        return this.storeUnpacked ?
            this.readUnpackedTarbal(name, signal) :
            this.readPackedTarbal(name, signal);
    }

    async writeTarball(name: string, { signal }: {
        signal: AbortSignal;
    }): Promise<Writable> {
        return this.storeUnpacked ?
            this.writeUnpackedTarball(name, signal) :
            this.writePackedTarball(name, signal);
    }

    async hasTarball(name: string): Promise<boolean> {
        return this.storeUnpacked ?
            this.hasUnpackedTarball(name) :
            this.hasPackedTarball(name);
    }

    private async readPackedTarbal(name: string, signal: AbortSignal): Promise<Readable> {
        this.trace('Reading packed tarbal @{name}', {name});

        try {
            const client = this.containerClient.getBlockBlobClient(this.getPath(name));
            
            const readStream = (await client.download(undefined, undefined, { abortSignal: signal })).readableStreamBody!;
            const readable = new Readable().wrap(readStream);
            signal.addEventListener('abort', () => readable.destroy(), {once: true})

            return readable;
        } catch(e: any) {
            this.error('Error while reading packed tarball @{name}', {name})
            throw e;
        }
    }
    
    private async readUnpackedTarbal(tarballName: string, signal: AbortSignal): Promise<Readable> {
        this.trace('Reading unpacked tarbal @{tarballName}', {tarballName});

        const dirName = this.getDirName(tarballName);
        const prefix = this.getPath(dirName);

        try {
            const packed = pack();
            const gzipped = packed.pipe(zlib.createGzip());

            for await (const { name } of this.containerClient.listBlobsFlat({ prefix })) {
                this.trace('Reading file @{name}', { name });

                const client = this.containerClient.getBlockBlobClient(this.getPath(name));
                const blob = (await client.download(undefined, undefined, { abortSignal: signal })).readableStreamBody!;

                const inPackageName = name.slice(prefix.length)

                const entry = packed.entry({ name: inPackageName })

                blob.pipe(entry);
                
                entry.end()
            }

            packed.finalize();

            return gzipped;
        } catch(e: any) {
            this.error('Error while reading unpacked tarball @{tarballName}', {tarballName})
            throw e;
        }
    }

    private async writePackedTarball(name: string, signal: AbortSignal): Promise<Writable> {
        this.trace('Writing packed tarbal @{name}', {name});

        try {
            const client = this.containerClient.getBlockBlobClient(this.getPath(name));

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

    private async writeUnpackedTarball(tarballName: string, signal: AbortSignal): Promise<Writable> {
        this.trace('Writing unpacked tarbal @{tarballName}', {tarballName});

        try {
            const tunnel = new PassThrough();
            signal.onabort = () => tunnel.destroy();
    
            tunnel
                .pipe(zlib.createGunzip())
                .pipe(extract())
                .on('entry', async ({name}, stream, next) => {
                    this.trace('Writing file @{name}', { name });
                    const dirName = this.getDirName(tarballName);
                    const client = this.containerClient.getBlockBlobClient(this.getPath(`${dirName}/${ name }`));
                    await client.uploadStream(stream, undefined, undefined, { abortSignal: signal });
                    next()
                });
    
    
            process.nextTick(() => tunnel.emit('open'));
    
            return tunnel;   
        } catch(e: any) {
            this.error('Error while writing unpacked tarball @{tarballName}', {tarballName});
            throw e;
        }
    }

    private async hasPackedTarball(name:string): Promise<boolean> {
        return this.containerClient.getBlockBlobClient(this.getPath(name)).exists();
    }

    private async hasUnpackedTarball(tarballName:string): Promise<boolean> {
        const dirName = this.getDirName(tarballName);
        const prefix = this.getPath(dirName);

        const iterator = this.containerClient
            .listBlobsFlat({ prefix })
            .byPage({ maxPageSize: 1 });

        const response = <ContainerListBlobFlatSegmentResponse>(await iterator.next()).value;

        return !!response.segment.blobItems.length;
    }

    private getPath(name: string): string {
        return `${this.packageName}/${name}`;
    }

    private getDirName(tarballName: string): string {
        return tarballName.split('-').pop()!.slice(0, -4);
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