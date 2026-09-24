import { defineConfig } from 'vite';
export default defineConfig({ base: './', server:{strictPort:true,port:5173}, preview:{port:4173,strictPort:true}, build:{target:'es2022'} });
