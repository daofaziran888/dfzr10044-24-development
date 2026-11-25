import { createRouter, createWebHistory } from 'vue-router'

import Overview from '@/views/Overview.vue'
import WorkbenchView from '@/views/WorkbenchView.vue'
import WriteJobDetail from '@/views/WriteJobDetail.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'overview', component: Overview },
    {
      path: '/write_job/:id',
      name: 'write-job-detail',
      component: WriteJobDetail,
      props: true,
      alias: ['/write-job/:id', '/write-jobs/:id'],
    },
    { path: '/:moduleKey', name: 'module', component: WorkbenchView, props: true },
  ],
})

export default router
