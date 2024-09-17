// api.js
import axios from 'axios';

export function createApiInstance(apiKey, apiPassword) {
    return axios.create({
        headers: {
            'accept': 'application/json',
            'Content-Type': 'application/json',
            'galaxy-ap-name': apiKey,       // API key
            'galaxy-ap-password': apiPassword // API password
        }
    });
}