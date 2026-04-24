import { render, screen } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { describe, expect, it } from 'vitest';
import { Sidebar } from './Sidebar';
import { NAVIGATION } from './navigation';

describe('Sidebar', () => {
  it('renders a link for every navigation item', () => {
    render(
      <MemoryRouter>
        <Sidebar />
      </MemoryRouter>,
    );
    for (const item of NAVIGATION) {
      expect(screen.getByRole('link', { name: new RegExp(item.label, 'i') })).toHaveAttribute(
        'href',
        item.to,
      );
    }
  });
});
