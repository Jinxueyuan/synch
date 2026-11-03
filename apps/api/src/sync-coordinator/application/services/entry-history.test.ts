import { describe, expect, it, vi } from "vitest";

import {
	createCoordinatorService,
	createMockCoordinatorSocketService,
	createTestCoordinatorState,
	testSocketSession,
	testWebSocket,
} from "../../test-helpers";

describe("coordinator entry version history", () => {
	it("lists entry history over the websocket control channel", async () => {
		const session = testSocketSession();
		const sender = testWebSocket();
		const socketService = createMockCoordinatorSocketService({
			readSocketSession: vi.fn(() => session),
			sendSocketMessage: vi.fn(),
		});
		const stateRepository = createTestCoordinatorState({
			listEntryVersions: vi.fn(() => [
				{
					version_id: "version-1",
					entry_id: "entry-1",
					source_revision: 2,
					op_type: "upsert" as const,
					blob_id: "blob-1",
					encrypted_metadata: "metadata",
					reason: "auto" as const,
					captured_at: 123,
				},
			]),
		});
		const service = createCoordinatorService({ stateRepository, socketService });

		await service.handleSocketMessage(
			sender,
			JSON.stringify({
				type: "list_entry_versions",
				requestId: "request-history",
				entryId: "entry-1",
				before: null,
				limit: 100,
			}),
		);

		expect(socketService.sendSocketMessage).toHaveBeenCalledWith("test", {
			type: "entry_versions_listed",
			requestId: "request-history",
			entryId: "entry-1",
			versions: [
				{
					versionId: "version-1",
					sourceRevision: 2,
					op: "upsert",
					blobId: "blob-1",
					encryptedMetadata: "metadata",
					reason: "auto",
					capturedAt: 123,
				},
			],
			hasMore: false,
			nextBefore: null,
		});
	});

	it("restores entry history with client-reencrypted metadata", async () => {
		const session = testSocketSession();
		const stateRepository = createTestCoordinatorState({
			readEntry: vi.fn(() => ({
				entry_id: "entry-1",
				revision: 2,
				blob_id: "blob-current",
				encrypted_metadata: "current-metadata",
				deleted: 0,
			})),
			readEntryVersion: vi.fn(() => ({
				version_id: "version-1",
				entry_id: "entry-1",
				source_revision: 1,
				op_type: "upsert" as const,
				blob_id: "blob-1",
				encrypted_metadata: "old-metadata",
				reason: "auto" as const,
				bucket_start_ms: 0,
				captured_at: 123,
				created_by_user_id: "user-2",
				created_by_local_vault_id: "local-vault-2",
			})),
		});
		const service = createCoordinatorService({ stateRepository });
		const commitMutation = vi.spyOn(
			service.mutationService,
			"commitMutation",
		).mockResolvedValue({
			message: {
				type: "commit_accepted",
				requestId: "request-restore",
				cursor: 42,
				entryId: "entry-1",
				revision: 3,
			},
			broadcastCursor: 42,
		});

		await expect(
			service.restoreEntryVersion(session, {
				type: "restore_entry_version",
				requestId: "request-restore",
				entryId: "entry-1",
				versionId: "version-1",
				baseRevision: 2,
				op: "upsert",
				blobId: "blob-1",
				encryptedMetadata: "reencrypted-metadata",
			}),
		).resolves.toEqual({
			message: {
				type: "entry_version_restored",
				requestId: "request-restore",
				entryId: "entry-1",
				restoredFromVersionId: "version-1",
				restoredFromRevision: 1,
				cursor: 42,
				revision: 3,
			},
			broadcastCursor: 42,
		});

		expect(commitMutation).toHaveBeenCalledWith(
			session,
			{
				type: "commit_mutation",
				requestId: "request-restore",
				mutation: expect.objectContaining({
					entryId: "entry-1",
					op: "upsert",
					baseRevision: 2,
					blobId: "blob-1",
					encryptedMetadata: "reencrypted-metadata",
				}),
			},
			{ forcedHistoryBefore: "before_restore" },
		);
	});

	it("restores entry history in batches with per-entry results", async () => {
		const session = testSocketSession();
		const stateRepository = createTestCoordinatorState({
			readEntry: vi.fn(() => ({
				entry_id: "entry-1",
				revision: 2,
				blob_id: "blob-current",
				encrypted_metadata: "current-metadata",
				deleted: 0,
			})),
			readEntryVersion: vi.fn(() => ({
				version_id: "version-1",
				entry_id: "entry-1",
				source_revision: 1,
				op_type: "upsert" as const,
				blob_id: "blob-1",
				encrypted_metadata: "old-metadata",
				reason: "auto" as const,
				bucket_start_ms: 0,
				captured_at: 123,
				created_by_user_id: "user-2",
				created_by_local_vault_id: "local-vault-2",
			})),
		});
		const service = createCoordinatorService({ stateRepository });
		const commitMutations = vi.spyOn(
			service.mutationService,
			"commitMutations",
		).mockResolvedValue({
			message: {
				type: "commit_mutations_committed",
				requestId: "request-restore-batch",
				cursor: 42,
				results: [
					{
						status: "accepted",
						mutationId: "mutation-1",
						cursor: 42,
						entryId: "entry-1",
						revision: 3,
					},
				],
			},
			broadcastCursor: 42,
		});

		await expect(
			service.restoreEntryVersions(session, {
				type: "restore_entry_versions",
				requestId: "request-restore-batch",
				restores: [
					{
						entryId: "entry-1",
						versionId: "version-1",
						baseRevision: 2,
						op: "upsert",
						blobId: "blob-1",
						encryptedMetadata: "reencrypted-metadata",
					},
				],
			}),
		).resolves.toEqual({
			message: {
				type: "entry_versions_restored",
				requestId: "request-restore-batch",
				cursor: 42,
				results: [
					{
						status: "accepted",
						entryId: "entry-1",
						restoredFromVersionId: "version-1",
						restoredFromRevision: 1,
						cursor: 42,
						revision: 3,
					},
				],
			},
			broadcastCursor: 42,
		});

		expect(commitMutations).toHaveBeenCalledWith(
			session,
			{
				type: "commit_mutations",
				requestId: "request-restore-batch",
				mutations: [
					expect.objectContaining({
						entryId: "entry-1",
						op: "upsert",
						baseRevision: 2,
						blobId: "blob-1",
						encryptedMetadata: "reencrypted-metadata",
					}),
				],
			},
			{ forcedHistoryBefore: "before_restore" },
		);
	});

	it("rejects stale client-assisted restores", async () => {
		const session = testSocketSession();
		const stateRepository = createTestCoordinatorState({
			readEntry: vi.fn(() => ({
				entry_id: "entry-1",
				revision: 3,
				blob_id: "blob-current",
				encrypted_metadata: "current-metadata",
				deleted: 0,
			})),
			readEntryVersion: vi.fn(() => ({
				version_id: "version-1",
				entry_id: "entry-1",
				source_revision: 1,
				op_type: "upsert" as const,
				blob_id: "blob-1",
				encrypted_metadata: "old-metadata",
				reason: "auto" as const,
				bucket_start_ms: 0,
				captured_at: 123,
				created_by_user_id: "user-2",
				created_by_local_vault_id: "local-vault-2",
			})),
		});
		const service = createCoordinatorService({ stateRepository });

		await expect(
			service.restoreEntryVersion(session, {
				type: "restore_entry_version",
				requestId: "request-restore",
				entryId: "entry-1",
				versionId: "version-1",
				baseRevision: 2,
				op: "upsert",
				blobId: "blob-1",
				encryptedMetadata: "reencrypted-metadata",
			}),
		).rejects.toMatchObject({
			code: "stale_revision",
			details: { expectedBaseRevision: 3, receivedBaseRevision: 2 },
		});
	});

	it("rejects restores when the client payload does not match the version", async () => {
		const session = testSocketSession();
		const stateRepository = createTestCoordinatorState({
			readEntry: vi.fn(() => ({
				entry_id: "entry-1",
				revision: 2,
				blob_id: "blob-current",
				encrypted_metadata: "current-metadata",
				deleted: 0,
			})),
			readEntryVersion: vi.fn(() => ({
				version_id: "version-1",
				entry_id: "entry-1",
				source_revision: 1,
				op_type: "upsert" as const,
				blob_id: "blob-1",
				encrypted_metadata: "old-metadata",
				reason: "auto" as const,
				bucket_start_ms: 0,
				captured_at: 123,
				created_by_user_id: "user-2",
				created_by_local_vault_id: "local-vault-2",
			})),
		});
		const service = createCoordinatorService({ stateRepository });

		await expect(
			service.restoreEntryVersion(session, {
				type: "restore_entry_version",
				requestId: "request-restore",
				entryId: "entry-1",
				versionId: "version-1",
				baseRevision: 2,
				op: "delete",
				blobId: null,
				encryptedMetadata: "reencrypted-metadata",
			}),
		).rejects.toMatchObject({
			code: "version_mismatch",
			details: { message: "restore payload does not match the requested version" },
		});
	});
});


it("blocks entry purge while paused even with legacy read access", async () => {
	const stateRepository = createTestCoordinatorState({
		readSyncPause: vi.fn(() => ({ pausedAt: 1, reason: "manual: stop" })),
	});
	const service = createCoordinatorService({ stateRepository });
	await expect(service.purgeDeletedEntries(testSocketSession(), {
		type: "purge_deleted_entries", requestId: "paused-purge",
		entries: [{ entryId: "deleted-entry", revision: 1 }],
	})).rejects.toMatchObject({ code: "sync_paused" });
	expect(stateRepository.readMutationEntry).not.toHaveBeenCalled();
});

describe("clearing a file's version history", () => {
	it("deletes every version of a live entry and reports the visible count", async () => {
		const stateRepository = createTestCoordinatorState({
			readMutationEntry: vi.fn(() => ({
				entryId: "entry-1",
				revision: 3,
				blobId: "blob-current",
				encryptedMetadata: "metadata",
				deleted: false,
				updatedSeq: 3,
				lastMutationId: null,
			})),
			countEntryVersions: vi.fn(() => 2),
			listBlobIds: vi.fn(() => ["blob-old-1", "blob-old-2"]),
		});
		const service = createCoordinatorService({ stateRepository });

		const result = await service.clearEntryVersions(testSocketSession(), {
			type: "clear_entry_versions",
			requestId: "clear-1",
			entryId: "entry-1",
		});

		expect(result.message).toEqual({
			type: "entry_versions_cleared",
			requestId: "clear-1",
			entryId: "entry-1",
			clearedCount: 2,
		});
		expect(stateRepository.countEntryVersions).toHaveBeenCalledWith(
			"entry-1",
			expect.any(Number),
		);
		expect(stateRepository.deleteEntryVersions).toHaveBeenCalledWith("entry-1");
	});

	it("rejects an unknown entry without touching stored versions", async () => {
		const stateRepository = createTestCoordinatorState();
		const service = createCoordinatorService({ stateRepository });

		await expect(
			service.clearEntryVersions(testSocketSession(), {
				type: "clear_entry_versions",
				requestId: "clear-missing",
				entryId: "missing-entry",
			}),
		).rejects.toMatchObject({ code: "not_found" });
		expect(stateRepository.deleteEntryVersions).not.toHaveBeenCalled();
	});

	it("rejects deleted entries because purging owns that lifecycle", async () => {
		const stateRepository = createTestCoordinatorState({
			readMutationEntry: vi.fn(() => ({
				entryId: "entry-deleted",
				revision: 4,
				blobId: null,
				encryptedMetadata: "metadata",
				deleted: true,
				updatedSeq: 4,
				lastMutationId: null,
			})),
		});
		const service = createCoordinatorService({ stateRepository });

		await expect(
			service.clearEntryVersions(testSocketSession(), {
				type: "clear_entry_versions",
				requestId: "clear-deleted",
				entryId: "entry-deleted",
			}),
		).rejects.toMatchObject({ code: "entry_deleted" });
		expect(stateRepository.deleteEntryVersions).not.toHaveBeenCalled();
	});

	it("blocks clearing while the vault is paused for repair", async () => {
		const stateRepository = createTestCoordinatorState({
			readSyncPause: vi.fn(() => ({ pausedAt: 1, reason: "manual: stop" })),
		});
		const service = createCoordinatorService({ stateRepository });

		await expect(
			service.clearEntryVersions(testSocketSession(), {
				type: "clear_entry_versions",
				requestId: "clear-paused",
				entryId: "entry-1",
			}),
		).rejects.toMatchObject({ code: "sync_paused" });
		expect(stateRepository.readMutationEntry).not.toHaveBeenCalled();
	});
});

it("clears entry history over the websocket control channel", async () => {
	const socketService = createMockCoordinatorSocketService({
		readSocketSession: vi.fn(() => testSocketSession()),
		sendSocketMessage: vi.fn(),
	});
	const stateRepository = createTestCoordinatorState({
		readMutationEntry: vi.fn(() => ({
			entryId: "entry-1",
			revision: 2,
			blobId: "blob-current",
			encryptedMetadata: "metadata",
			deleted: false,
			updatedSeq: 2,
			lastMutationId: null,
		})),
		countEntryVersions: vi.fn(() => 1),
	});
	const service = createCoordinatorService({ stateRepository, socketService });

	await service.handleSocketMessage(
		testWebSocket(),
		JSON.stringify({
			type: "clear_entry_versions",
			requestId: "request-clear",
			entryId: "entry-1",
		}),
	);

	expect(socketService.sendSocketMessage).toHaveBeenCalledWith("test", {
		type: "entry_versions_cleared",
		requestId: "request-clear",
		entryId: "entry-1",
		clearedCount: 1,
	});
});
