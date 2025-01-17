static __thread void* context;

extern inline void* get_current_context() {
    return context;
}

extern inline void set_current_context(void* x) {
    context = x;
}

