export default class Sorter {
    constructor(rawData) {
        this.rawData = rawData;
        this.parsedData = this.parseData();
    }

    parseData() {
        const parts = this.rawData.trim().split(';');

        // Handle missing parts
        if (parts.length < 9) {
            return [];
        }

        // Name parts
        const fullNameParts = parts[2].trim().split(' ');
        const firstName = fullNameParts[0] || null;
        const lastName = fullNameParts[fullNameParts.length - 1] || null;
        const middleName = fullNameParts.length > 2 ? fullNameParts.slice(1, fullNameParts.length - 1).join(' ') : null;

        // Phones and Emails
        const phonesAndEmails = this.splitPhonesAndEmails(parts[7], parts[8]);

        return [{
            FirstName: firstName,
            MiddleName: middleName,
            LastName: lastName,
            Dob: parts[1].trim(), // Assuming it's already in mm/dd/yyyy
            Age: this.calculateAge(parts[1].trim()),
            Address: {
                AddressLine1: parts[3].trim(),
                AddressLine2: `${parts[4].trim()}, ${parts[5].trim()} ${parts[6].trim()}`
            },
            Phone: phonesAndEmails.phones.length > 0 ? phonesAndEmails.phones[0] : null,
            Email: phonesAndEmails.emails.length > 0 ? phonesAndEmails.emails[0] : null
        }];
    }

    splitPhonesAndEmails(phonePart, emailPart) {
        const phoneRegex = /^\d{10}$/;
        const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

        const phones = [];
        const emails = [];

        if (phonePart) {
            const potentialPhones = phonePart.split(/[\|;]/);
            potentialPhones.forEach(item => {
                const cleaned = item.replace(/\D/g, '');
                if (phoneRegex.test(cleaned)) {
                    phones.push(cleaned);
                }
            });
        }

        if (emailPart) {
            const potentialEmails = emailPart.split(/[\|;]/);
            potentialEmails.forEach(item => {
                if (emailRegex.test(item.trim())) {
                    emails.push(item.trim());
                }
            });
        }

        return { phones, emails };
    }

    calculateAge(birthDateStr) {
        const [monthStr, dayStr, yearStr] = birthDateStr.split('/');
        const month = parseInt(monthStr, 10) - 1;
        const day = parseInt(dayStr, 10);
        const year = parseInt(yearStr, 10);
        const birthDate = new Date(year, month, day);
        if (isNaN(birthDate.getTime())) {
            return null;
        }
        const today = new Date();
        let age = today.getFullYear() - birthDate.getFullYear();
        const m = today.getMonth() - birthDate.getMonth();
        if (m < 0 || (m === 0 && today.getDate() < birthDate.getDate())) {
            age--;
        }
        return age;
    }
}