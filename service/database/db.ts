import { SendEmailRequest } from "@aws-sdk/client-sesv2"
import { NotificationEvent } from "../../lib/core/aws-utils"
import { MailgunMessage } from "@/types/mailgun"
import { safeStringify } from "../../lib/core/common"
import { PrismaClient } from "../../lib/generated"

export const prisma = new PrismaClient()

export async function createNewsletterBatchEntry(siteId: string, message: MailgunMessage) {
    const batchId = message["v:email-id"]
    const contents = safeStringify(message)
    const fromEmail = message.from
    return prisma.newsletterBatch.create({
        select: { id: true },
        data: {
            siteId,
            batchId,
            contents,
            fromEmail,
        },
    })
}

export async function createNewsletterEntry(messageId: string, batchId: string, payload: SendEmailRequest) {
    const toEmail = payload.Destination?.ToAddresses?.join("") || ""
    return prisma.newsletterMessages.create({
        data: {
            newsletterBatchId: batchId,
            formatedContents: safeStringify(payload),
            toEmail,
            messageId,
        },
    })
}

export async function createNewsletterErrorEntry(
    messageId: string,
    errorMessage: string,
    batchId: string,
    payload: SendEmailRequest
) {
    const toEmail = payload.Destination?.ToAddresses?.join("") || ""
    return prisma.newsletterErrors.create({
        data: {
            error: errorMessage,
            newsletterBatchId: batchId,
            messageId: messageId,
            formatedContents: safeStringify(payload),
            toEmail
        },
    })
}

export function saveNewsletterNotification(event: NotificationEvent) {
    return prisma.newsletterNotifications.create({
        data: {
            messageId: event.messageId,
            rawEvent: event.raw,
            type: event.type,
            notificationId: event.notificationId,
            timestamp: event.timestamp,
        },
    })
}

/**
 * Returns the set of recipient addresses that were already handled (sent or
 * recorded as failed) for a newsletter batch. Used to make batch processing
 * idempotent: SQS is at-least-once, so the same batch can be delivered again
 * (visibility timeout expired, another consumer touched it, ...). Recipients
 * in this set must not receive the email a second time.
 */
export async function getProcessedRecipientsForBatch(newsletterBatchId: string): Promise<Set<string>> {
    const [sent, failed] = await Promise.all([
        prisma.newsletterMessages.findMany({ where: { newsletterBatchId }, select: { toEmail: true } }),
        prisma.newsletterErrors.findMany({ where: { newsletterBatchId }, select: { toEmail: true } }),
    ])
    return new Set([...sent, ...failed].map((r) => r.toEmail))
}

export async function getNewsletterContent(id: string) {
    const result = await prisma.newsletterBatch.findUnique({
        where: { id },
        select: { contents: true }
    })
    return result && result.contents ? JSON.parse(result.contents) : null
}

export async function saveSystemEmailEvent(event: NotificationEvent) {
    return prisma.newsletterNotifications.create({
        data: {
            messageId: event.messageId,
            rawEvent: event.raw,
            type: event.type,
            notificationId: event.notificationId,
            timestamp: event.timestamp,
        },
    })
}