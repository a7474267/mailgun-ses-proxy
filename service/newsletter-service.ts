import { preparePayload } from "../lib/core/aws-utils"
import { safeStringify } from "../lib/core/common"
import { SendEmailCommand } from "@aws-sdk/client-sesv2"
import { DeleteMessageCommand, Message, SendMessageCommand } from "@aws-sdk/client-sqs"
import { randomUUID } from "crypto"
import {
    createNewsletterBatchEntry,
    createNewsletterEntry,
    createNewsletterErrorEntry,
    getNewsletterContent,
    getProcessedRecipientsForBatch,
} from "./database/db"
import { QUEUE_URL, sesNewsletterClient, sqsClient } from "./aws/awsHelper"
import logger from "../lib/core/logger"

const log = logger.child({ service: "service:newsletter-service" })

export async function addNewsletterToQueue(message: any, siteId: string, auth: any) {
    if (!message) throw new Error("Message body is empty or invalid.")
    log.debug({ message }, "sending message body to SQS")

    const result = await createNewsletterBatchEntry(siteId, message)
    const params = {
        QueueUrl: QUEUE_URL.NEWSLETTER,
        MessageBody: String(result.id),
        MessageAttributes: {
            siteId: {
                DataType: "String",
                StringValue: siteId,
            },
            from: {
                DataType: "String",
                StringValue: message.from,
            },
        },
    }
    const command = new SendMessageCommand(params)
    const response = await sqsClient().send(command)
    return { batchId: message["v:email-id"], messageId: response.MessageId }
}

type SendMailResult = { batchId: string; sent: number; skipped: number; failed: number }

/**
 * Sends one newsletter batch, one SES request per recipient.
 *
 * Idempotent: recipients that already have a NewsletterMessages / NewsletterErrors
 * row for this batch are skipped, so a redelivered SQS message (or a retry after a
 * partial failure) only sends to the recipients that are still missing.
 * A failure for one recipient is recorded and does not abort the rest of the batch.
 */
async function sendMail(siteId: string, dbId: string): Promise<SendMailResult> {
    const contents = await getNewsletterContent(dbId)
    if (!contents) throw new Error(`newsletter batch ${dbId} not found in database`)

    const sendEmailRequests = preparePayload(contents, siteId)
    const batchId = contents["v:email-id"]
    const alreadyProcessed = await getProcessedRecipientsForBatch(dbId)
    const result: SendMailResult = { batchId, sent: 0, skipped: 0, failed: 0 }

    for (const requests of sendEmailRequests) {
        const toEmail = requests.Destination?.ToAddresses?.join("") || ""
        if (alreadyProcessed.has(toEmail)) {
            result.skipped++
            continue
        }
        try {
            const cmd = new SendEmailCommand(requests)
            const resp = await sesNewsletterClient().send(cmd)
            const messageId = resp.MessageId as string
            await createNewsletterEntry(messageId, dbId, requests)
            result.sent++
            log.info({ messageId, siteId }, "email sent")
        } catch (e) {
            result.failed++
            log.error({ error: e, toEmail, batchId, siteId }, "error occurred at sendMail")
            try {
                await createNewsletterErrorEntry(randomUUID(), String(e), dbId, requests)
            } catch (dbError) {
                log.error({ error: dbError, toEmail, batchId }, "failed to record newsletter error entry")
            }
        }
    }
    if (result.skipped > 0) {
        log.warn({ ...result, siteId, dbId }, "batch was (partially) processed before; skipped already-handled recipients")
    }
    return result
}

async function deleteFromQueue(message: Message) {
    await sqsClient().send(
        new DeleteMessageCommand({
            QueueUrl: QUEUE_URL.NEWSLETTER,
            ReceiptHandle: message.ReceiptHandle,
        })
    )
}

export async function validateAndSend(message: Message) {
    if (!message.MessageAttributes || !message.Body) {
        log.error({ message: safeStringify(message) }, "invalid message")
        return
    }

    const siteId = message.MessageAttributes["siteId"]?.StringValue
    const from = message.MessageAttributes["from"]?.StringValue

    if (!siteId || !from) {
        log.error({ message: safeStringify(message) }, "missing required message attributes")
        return
    }

    const dbId = message.Body
    try {
        const receiveCount = parseInt(message.Attributes?.ApproximateReceiveCount || "0")
        if (receiveCount > 5) {
            await deleteFromQueue(message)
            throw new Error("Message has been received more than 5 times, skipping it.")
        }

        // A batch that does not exist in this database belongs to another proxy
        // instance sharing the same queue. Leave the message alone so the owner
        // can pick it up once the visibility timeout expires.
        const contents = await getNewsletterContent(dbId)
        if (!contents) {
            log.warn({ dbId, siteId, receiveCount }, "batch not found in this database, leaving message for another consumer")
            return
        }

        const result = await sendMail(siteId, dbId)
        log.info({ ...result, siteId, dbId }, "batch processed")

        // Every recipient is now either sent or recorded as failed; a redelivery
        // would be skipped by the idempotency check, so the message can be removed.
        await deleteFromQueue(message)
    } catch (e) {
        log.error({ error: e, dbId }, "error occurred at validateAndSend")
    }
}
